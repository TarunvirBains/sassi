//! Criterion harness for Task 17 benchmark baselines.
//!
//! Scope:
//! - public Punnu/Sassi path coverage
//! - insert throughput
//! - hot get
//! - BasicPredicate scope
//! - closure/mixed MemQ scope
//! - direct apply_delta
//! - get_or_fetch hit and miss/coalescing paths
//! - get_or_fetch_many
//! - sampled-LRU pressure
//! - wire/serde JSON round-trips
//! - file/backend paths if serde is enabled
//! - TTL/scheduler-heavy paths
//! - Sassi::all_impl
//! - read-under-write stress
//!
//! These are baseline, same-host comparison points. No absolute
//! throughput claims are derived from these numbers.

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::{future::join_all, join};
use sassi::{Cacheable, DeltaResult, Field, MemQ, Punnu, PunnuConfig, Sassi};
#[cfg(feature = "serde")]
use sassi::{
    FileBackend,
    wire::{from_slice, to_vec},
};
#[cfg(feature = "serde")]
use std::sync::atomic::AtomicU64;
use tokio::runtime::{Builder, Runtime};

const SAMPLE_INSERT_COUNT: usize = 2_000;
const HOT_SCOPE_COUNT: usize = 8_000;
const LRU_PRESSURE_SIZE: usize = 256;
const LRU_PRESSURE_INSERTS: usize = 2_400;
const ORCHESTRATOR_COUNT: usize = 4_000;
const COALESCE_CONCURRENCY: usize = 32;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct BenchItem {
    id: u64,
    tenant_id: u32,
    score: u32,
    version: u64,
    active: bool,
}

#[derive(Default)]
struct BenchItemFields {
    #[allow(dead_code)]
    id: Field<BenchItem, u64>,
    tenant_id: Field<BenchItem, u32>,
    score: Field<BenchItem, u32>,
    #[allow(dead_code)]
    version: Field<BenchItem, u64>,
    #[allow(dead_code)]
    active: Field<BenchItem, bool>,
}

impl Cacheable for BenchItem {
    type Id = u64;
    type Fields = BenchItemFields;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {
        BenchItemFields {
            id: Field::new("id", |item| &item.id),
            tenant_id: Field::new("tenant_id", |item| &item.tenant_id),
            score: Field::new("score", |item| &item.score),
            version: Field::new("version", |item| &item.version),
            active: Field::new("active", |item| &item.active),
        }
    }
}

impl sassi::DeltaSyncCacheable for BenchItem {
    type Watermark = u64;

    fn watermark(&self) -> Self::Watermark {
        self.version
    }
}

impl BenchItem {
    fn new(id: u64) -> Self {
        Self {
            id,
            tenant_id: (id % 7) as u32,
            score: ((id * 37) % 1000) as u32,
            version: id,
            active: id.is_multiple_of(2),
        }
    }
}

fn build_items(start: u64, count: usize) -> Vec<BenchItem> {
    (start..start + count as u64).map(BenchItem::new).collect()
}

#[cfg(feature = "serde")]
static BACKEND_DIR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("Tokio current-thread runtime should build")
}

fn populate_cache(runtime: &Runtime, pool: &Punnu<BenchItem>, start: u64, count: usize) {
    for item in build_items(start, count) {
        runtime
            .block_on(pool.insert(item))
            .expect("benchmark population insert should work");
    }
}

fn populate_cache_with_ttl(
    runtime: &Runtime,
    pool: &Punnu<BenchItem>,
    start: u64,
    count: usize,
    ttl: Duration,
) {
    for item in build_items(start, count) {
        runtime
            .block_on(pool.insert_with_ttl(item, ttl))
            .expect("benchmark ttl insert should work");
    }
}

#[cfg(feature = "serde")]
fn unique_backend_path(prefix: &str) -> PathBuf {
    let sequence = BACKEND_DIR_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let mut out = std::env::temp_dir();
    out.push(format!("sassi_bench_{}_{prefix}_{sequence}", process::id()));
    out
}

#[cfg(feature = "serde")]
fn remove_backend_dir(dir: &PathBuf) {
    std::fs::remove_dir_all(dir).expect("temp backend dir should be removed");
}

fn bench_gating_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("gating");

    let runtime = runtime();

    group.throughput(Throughput::Elements(SAMPLE_INSERT_COUNT as u64));
    let samples = build_items(0, SAMPLE_INSERT_COUNT);
    group.bench_function("insert_no_eviction", |b| {
        b.iter_batched(
            || Punnu::<BenchItem>::builder().build(),
            |pool| {
                for item in samples.iter().cloned() {
                    runtime
                        .block_on(pool.insert(item))
                        .expect("insert should not fail in benchmark");
                }
                black_box(pool.len());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("hot_get", |b| {
        let pool = Punnu::<BenchItem>::builder()
            .config(PunnuConfig {
                lru_size: 10_000,
                ..PunnuConfig::default()
            })
            .build();
        populate_cache(&runtime, &pool, 0, HOT_SCOPE_COUNT);
        let ids: Vec<u64> = (0..HOT_SCOPE_COUNT as u64).collect();
        assert!(pool.get(&0).is_some());

        b.iter(|| {
            let mut hits = 0usize;
            for id in &ids {
                if pool.get(id).is_some() {
                    hits += 1;
                }
            }
            assert_eq!(hits, HOT_SCOPE_COUNT);
            black_box(hits);
        });
    });

    group.bench_function("scope_basic_predicate", |b| {
        let pool = Punnu::<BenchItem>::builder().build();
        populate_cache(&runtime, &pool, 0, HOT_SCOPE_COUNT);
        let fields = BenchItem::fields();
        let predicate = fields.score.gte(500) & fields.tenant_id.lt(4);
        let expected_min = HOT_SCOPE_COUNT / 8;
        b.iter(|| {
            let result = pool
                .scope(vec![MemQ::filter_basic(predicate.clone())])
                .collect();
            assert!(result.len() >= expected_min);
            black_box(result.len());
        });
    });

    group.bench_function("scope_closure_mixed_memq", |b| {
        let pool = Punnu::<BenchItem>::builder().build();
        populate_cache(&runtime, &pool, 0, HOT_SCOPE_COUNT);
        let fields = BenchItem::fields();
        b.iter(|| {
            let values = pool
                .scope(vec![
                    MemQ::filter_basic(fields.tenant_id.eq(2)),
                    MemQ::filter(|item: &BenchItem| item.active && item.score.is_multiple_of(2)),
                    MemQ::map_arc(|value| value),
                    MemQ::take(128),
                ])
                .collect();
            assert!(!values.is_empty());
            black_box(values.len());
        });
    });

    group.bench_function("apply_delta_direct", |b| {
        let tombstones = HashSet::from([3_u64, 11_u64]);
        let tombstone_count = tombstones.len();
        b.iter_batched(
            || {
                let pool = Punnu::<BenchItem>::builder().build();
                populate_cache(&runtime, &pool, 0, SAMPLE_INSERT_COUNT / 2);
                pool
            },
            |pool| {
                let stats = pool.apply_delta(DeltaResult::new(
                    build_items(5_000, SAMPLE_INSERT_COUNT / 2),
                    tombstones.clone(),
                ));
                assert!(stats.applied_items > 0);
                assert!(stats.tombstones_evicted <= tombstone_count);
                black_box(stats);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("get_or_fetch_hit", |b| {
        let pool = Punnu::<BenchItem>::builder().build();
        populate_cache(&runtime, &pool, 0, HOT_SCOPE_COUNT);
        let requested = 73_u64;
        let fetch_invocations = Arc::new(AtomicUsize::new(0));

        b.iter(|| {
            let fetch_invocations_for_fetch = Arc::clone(&fetch_invocations);
            runtime.block_on(async {
                let hit = pool
                    .get_or_fetch(&requested, move |id| {
                        fetch_invocations_for_fetch.fetch_add(1, Ordering::SeqCst);
                        async move { Ok::<_, sassi::FetchError>(Some(BenchItem::new(id))) }
                    })
                    .await
                    .expect("get_or_fetch should be cached");
                black_box(hit.is_some());
                assert_eq!(hit.map(|value| value.id), Some(requested));
            });
            assert_eq!(fetch_invocations.load(Ordering::SeqCst), 0);
        });
    });

    group.finish();
}

fn bench_nongating_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("non_gating");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    let runtime = runtime();

    group.bench_function("sampled_lru_pressure", |b| {
        b.iter_batched(
            || {
                Punnu::<BenchItem>::builder()
                    .config(PunnuConfig {
                        lru_size: LRU_PRESSURE_SIZE,
                        ..PunnuConfig::default()
                    })
                    .build()
            },
            |pool| {
                for id in 0..LRU_PRESSURE_INSERTS as u64 {
                    runtime
                        .block_on(pool.insert(BenchItem::new(id)))
                        .expect("insert should work during lru benchmark");
                }
                assert_eq!(pool.len(), LRU_PRESSURE_SIZE);
                black_box(pool.len());
            },
            BatchSize::SmallInput,
        );
    });

    #[cfg(feature = "serde")]
    {
        group.bench_function("serde_json_wire_roundtrip_to_vec", |b| {
            let item = BenchItem::new(42);
            b.iter(|| {
                let bytes = to_vec(&item).expect("wire serialization should work");
                black_box(bytes);
            });
        });

        group.bench_function("serde_json_wire_roundtrip_from_slice", |b| {
            let item = BenchItem::new(42);
            let bytes = to_vec(&item).expect("wire serialization should work");
            b.iter(|| {
                let restored: BenchItem =
                    from_slice(&bytes).expect("wire deserialization should work");
                assert_eq!(restored.version, item.version);
                black_box(restored.id);
            });
        });
    }

    #[cfg(feature = "serde")]
    {
        group.bench_function("backend_file_roundtrip_get_async", |b| {
            let mut warm: u64 = 0;
            b.iter_batched(
                || {
                    let namespace = format!("ns-{warm}");
                    warm += 1;
                    let dir = unique_backend_path("bench_get_async");
                    std::fs::create_dir_all(&dir).expect("temp backend dir should create");

                    let seed_pool = {
                        let _guard = runtime.enter();
                        Punnu::<BenchItem>::builder()
                            .config(PunnuConfig {
                                namespace: Some(namespace.clone()),
                                ..PunnuConfig::default()
                            })
                            .backend(FileBackend::new(&dir))
                            .build()
                    };
                    runtime
                        .block_on(seed_pool.insert(BenchItem::new(1_234)))
                        .expect("seed insert should work");
                    drop(seed_pool);

                    let _guard = runtime.enter();
                    let pool = Punnu::<BenchItem>::builder()
                        .config(PunnuConfig {
                            namespace: Some(namespace),
                            ..PunnuConfig::default()
                        })
                        .backend(FileBackend::new(&dir))
                        .build();
                    (pool, dir)
                },
                |(pool, dir)| {
                    let fetched = runtime.block_on(async {
                        let value = pool
                            .get_async(&1_234)
                            .await
                            .expect("backend get_async should work");
                        assert!(value.is_some());
                        value
                    });
                    black_box(fetched.is_some());
                    remove_backend_dir(&dir);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function("backend_file_roundtrip_insert", |b| {
            b.iter_batched(
                || {
                    let dir = unique_backend_path("bench_insert");
                    let namespace = process::id().to_string();
                    std::fs::create_dir_all(&dir).expect("temp backend dir should create");
                    let _guard = runtime.enter();
                    let pool = Punnu::<BenchItem>::builder()
                        .config(PunnuConfig {
                            namespace: Some(namespace),
                            ..PunnuConfig::default()
                        })
                        .backend(FileBackend::new(&dir))
                        .build();
                    (pool, dir)
                },
                |(pool, dir)| {
                    let inserted = runtime
                        .block_on(pool.insert(BenchItem::new(1)))
                        .expect("backend insert should work");
                    assert_eq!(inserted.id(), 1);
                    black_box(inserted.id());
                    remove_backend_dir(&dir);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.bench_function("ttl_scheduler_like_path", |b| {
        b.iter_batched(
            || {
                let _guard = runtime.enter();
                let pool = Punnu::<BenchItem>::builder()
                    .config(PunnuConfig {
                        default_ttl: Some(Duration::from_millis(1)),
                        ttl_sweep_interval: Some(Duration::from_millis(1)),
                        ..PunnuConfig::default()
                    })
                    .build();
                populate_cache_with_ttl(&runtime, &pool, 0, 256, Duration::from_millis(1));
                pool
            },
            |pool| {
                runtime.block_on(async {
                    tokio::time::sleep(Duration::from_millis(3)).await;
                    let before = pool.get(&1);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    let after = pool.get(&1);
                    assert!(before.is_none());
                    assert!(after.is_none());
                    black_box((before.is_none(), after.is_none()));
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("get_or_fetch_many", |b| {
        let pool = Punnu::<BenchItem>::builder().build();
        populate_cache(&runtime, &pool, 0, 200);
        let requested = (0..400_u64).collect::<Vec<_>>();

        b.iter(|| {
            let got = runtime
                .block_on(
                    pool.get_or_fetch_many(&requested, move |missing| async move {
                        Ok(missing.into_iter().map(BenchItem::new).collect())
                    }),
                )
                .expect("get_or_fetch_many should work");
            assert_eq!(got.len(), requested.len());
            black_box(got.len());
        });
    });

    group.bench_function("get_or_fetch_many_coalesced_single_item", |b| {
        let target = 777_777_u64;

        b.iter_batched(
            || Punnu::<BenchItem>::builder().build(),
            |pool| {
                let fetch_calls = Arc::new(AtomicUsize::new(0));

                let fetched = runtime.block_on({
                    let pool = pool.clone();
                    let calls = Arc::clone(&fetch_calls);
                    async move {
                        let tasks = (0..COALESCE_CONCURRENCY).map(|_| {
                            let pool = pool.clone();
                            let calls = Arc::clone(&calls);
                            async move {
                                let calls = Arc::clone(&calls);
                                pool.get_or_fetch(&target, move |id| {
                                    calls.fetch_add(1, Ordering::SeqCst);
                                    async move {
                                        tokio::task::yield_now().await;
                                        Ok::<_, sassi::FetchError>(Some(BenchItem::new(id)))
                                    }
                                })
                                .await
                                .expect("coalesced get_or_fetch should work")
                                .is_some()
                            }
                        });
                        let results = join_all(tasks).await;
                        assert!(results.iter().all(|is_some| *is_some));
                        black_box(results.len());
                        results.len()
                    }
                });

                assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);
                assert_eq!(fetched, COALESCE_CONCURRENCY);
                black_box(fetched);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("read_under_write_stress", |b| {
        b.iter_batched(
            || {
                let pool = Punnu::<BenchItem>::builder().build();
                populate_cache(&runtime, &pool, 0, HOT_SCOPE_COUNT);
                pool
            },
            |pool| {
                let write_count = 1_000usize;
                runtime.block_on(async {
                    let reader_task = async {
                        let mut seen = VecDeque::with_capacity(16);
                        for idx in 0..write_count {
                            let id = (idx as u64 * 7) % HOT_SCOPE_COUNT as u64;
                            seen.push_front(pool.get(&id).is_some());
                            if seen.len() > 16 {
                                let _ = seen.pop_back();
                            }
                            tokio::task::yield_now().await;
                        }
                        seen.len()
                    };

                    let writer_task = async {
                        for idx in 0..write_count {
                            pool.insert(BenchItem::new(
                                HOT_SCOPE_COUNT as u64 + idx as u64 + 1_000_000,
                            ))
                            .await
                            .expect("writer insert should work");
                            tokio::task::yield_now().await;
                        }
                        0usize
                    };
                    let (read_count, written) = join!(reader_task, writer_task);
                    assert_eq!(written, 0);
                    black_box(read_count);
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_orchestrator_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("non_gating_orchestrator");
    let runtime = runtime();

    group.bench_function("all_impl", |b| {
        let orchestrator = {
            let pool = Punnu::<BenchItem>::builder().build();
            populate_cache(&runtime, &pool, 0, ORCHESTRATOR_COUNT);
            let mut orchestrator = Sassi::new();
            orchestrator.register::<BenchItem>(Arc::new(pool));
            orchestrator
        };

        b.iter(|| {
            let impls = orchestrator.all_impl::<dyn BenchLabel>();
            assert!(!impls.is_empty());
            let mut ids = impls
                .into_iter()
                .map(|item| item.label())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            black_box(ids.len());
        });
    });

    group.finish();
}

trait BenchLabel: Send + Sync {
    fn label(&self) -> u64;
}

#[sassi::trait_impl]
impl BenchLabel for BenchItem {
    fn label(&self) -> u64 {
        self.version
    }
}

fn bench_main(c: &mut Criterion) {
    bench_gating_group(c);
    bench_nongating_group(c);
    bench_orchestrator_group(c);
}

criterion_group!(benches, bench_main);
criterion_main!(benches);
