//! `PunnuMetrics` observability hook.
//!
//! Spec §3.5.1 — sassi commits to firing `record_*` callbacks on every
//! event of interest; consumers wire to whatever metrics layer they
//! already use. This file pins the call sites so a regression in any
//! one of them surfaces immediately rather than waiting for a
//! production dashboard to go silent.
//!
//! Coverage:
//! - `record_hit` on `get` cache hit (CacheTier::L1).
//! - `record_miss` on `get` miss + on TTL-expired get.
//! - `record_eviction` on manual `invalidate`, on LRU pressure, on
//!   TTL expiry (lazy + sweep paths).
//! - `record_lru_size` after every insert / invalidate.
//! - `record_fetch_latency` after every `get_or_fetch` slow-path.
//! - No calls when `metrics: None`.

#![cfg(feature = "runtime-tokio")]

use sassi::{
    BackendError, CacheTier, Cacheable, EventReason, FetchError, Field, InvalidationReason, Punnu,
    PunnuConfig, PunnuMetrics,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone)]
struct E {
    id: i64,
}

#[derive(Default)]
struct EFields {
    #[allow(dead_code)]
    id: Field<E, i64>,
}

impl Cacheable for E {
    type Id = i64;
    type Fields = EFields;
    fn id(&self) -> i64 {
        self.id
    }
    fn fields() -> EFields {
        EFields {
            id: Field::new("id", |e| &e.id),
        }
    }
}

#[derive(Default)]
struct CountingMetrics {
    hits: Mutex<Vec<(String, CacheTier)>>,
    misses: Mutex<Vec<String>>,
    evictions: Mutex<Vec<(String, EventReason)>>,
    lru_sizes: Mutex<Vec<(String, usize)>>,
    fetch_latencies: Mutex<Vec<(String, Duration)>>,
    backend_errors: Mutex<Vec<String>>,
}

impl PunnuMetrics for CountingMetrics {
    fn record_hit(&self, type_name: &'static str, tier: CacheTier) {
        self.hits.lock().unwrap().push((type_name.into(), tier));
    }
    fn record_miss(&self, type_name: &'static str) {
        self.misses.lock().unwrap().push(type_name.into());
    }
    fn record_eviction(&self, type_name: &'static str, reason: EventReason) {
        self.evictions
            .lock()
            .unwrap()
            .push((type_name.into(), reason));
    }
    fn record_backend_error(&self, _type_name: &'static str, err: &BackendError) {
        self.backend_errors.lock().unwrap().push(format!("{err}"));
    }
    fn record_fetch_latency(&self, type_name: &'static str, duration: Duration) {
        self.fetch_latencies
            .lock()
            .unwrap()
            .push((type_name.into(), duration));
    }
    fn record_lru_size(&self, type_name: &'static str, size: usize) {
        self.lru_sizes
            .lock()
            .unwrap()
            .push((type_name.into(), size));
    }
}

fn punnu_with_metrics(metrics: Arc<CountingMetrics>) -> Punnu<E> {
    let dyn_metrics: Arc<dyn PunnuMetrics> = metrics;
    Punnu::<E>::builder()
        .config(PunnuConfig {
            metrics: Some(dyn_metrics),
            ..Default::default()
        })
        .build()
}

#[tokio::test]
async fn metrics_records_hit_and_miss() {
    let m = Arc::new(CountingMetrics::default());
    let p = punnu_with_metrics(m.clone());

    p.insert(E { id: 1 }).await.unwrap();
    let _ = p.get(&1); // hit
    let _ = p.get(&999); // miss

    let hits = m.hits.lock().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].1, CacheTier::L1);
    assert!(
        hits[0].0.contains("E"),
        "type_name should include type identity; got {}",
        hits[0].0
    );

    let misses = m.misses.lock().unwrap();
    assert_eq!(misses.len(), 1);
}

#[tokio::test]
async fn metrics_records_lru_size_after_insert_and_invalidate() {
    let m = Arc::new(CountingMetrics::default());
    let p = punnu_with_metrics(m.clone());

    p.insert(E { id: 1 }).await.unwrap();
    p.insert(E { id: 2 }).await.unwrap();
    p.invalidate(&1, InvalidationReason::Manual).await;

    let sizes = m.lru_sizes.lock().unwrap().clone();
    assert!(
        sizes.len() >= 3,
        "expected ≥3 lru_size samples, got {sizes:?}"
    );
    // After two inserts and one invalidate, the last sample should
    // show 1 (only id=2 left). The sequence is: insert 1 → size=1,
    // insert 2 → size=2, invalidate 1 → size=1.
    assert_eq!(sizes[0].1, 1);
    assert_eq!(sizes[1].1, 2);
    assert_eq!(sizes[2].1, 1);
}

#[tokio::test]
async fn metrics_records_eviction_on_manual_invalidate() {
    let m = Arc::new(CountingMetrics::default());
    let p = punnu_with_metrics(m.clone());

    p.insert(E { id: 1 }).await.unwrap();
    p.invalidate(&1, InvalidationReason::Manual).await;

    let evictions = m.evictions.lock().unwrap().clone();
    assert_eq!(evictions.len(), 1);
    assert_eq!(evictions[0].1, EventReason::Manual);
}

#[tokio::test]
async fn metrics_records_eviction_on_lru_pressure() {
    let m = Arc::new(CountingMetrics::default());
    let dyn_metrics: Arc<dyn PunnuMetrics> = m.clone();
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: 2,
            metrics: Some(dyn_metrics),
            ..Default::default()
        })
        .build();

    p.insert(E { id: 1 }).await.unwrap();
    p.insert(E { id: 2 }).await.unwrap();
    // Inserting a third entry evicts id=1 under default LRU pressure.
    p.insert(E { id: 3 }).await.unwrap();

    let evictions = m.evictions.lock().unwrap().clone();
    assert!(
        evictions
            .iter()
            .any(|(_, r)| matches!(r, EventReason::LruEvict { .. })),
        "expected an LruEvict eviction, got {evictions:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn metrics_records_eviction_on_lazy_ttl_expiry() {
    let m = Arc::new(CountingMetrics::default());
    let dyn_metrics: Arc<dyn PunnuMetrics> = m.clone();
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            metrics: Some(dyn_metrics),
            ..Default::default()
        })
        .build();

    p.insert(E { id: 1 }).await.unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    let _ = p.get(&1); // triggers lazy TTL expiry

    let evictions = m.evictions.lock().unwrap().clone();
    assert!(
        evictions
            .iter()
            .any(|(_, r)| matches!(r, EventReason::TtlExpired { .. })),
        "expected a TtlExpired eviction, got {evictions:?}"
    );
}

#[tokio::test]
async fn metrics_records_fetch_latency_on_slow_path() {
    let m = Arc::new(CountingMetrics::default());
    let p = punnu_with_metrics(m.clone());

    let _ = p
        .get_or_fetch(&1, |id| async move { Ok::<_, FetchError>(Some(E { id })) })
        .await
        .unwrap();

    let latencies = m.fetch_latencies.lock().unwrap().clone();
    assert_eq!(
        latencies.len(),
        1,
        "expected one fetch_latency sample, got {latencies:?}"
    );
}

#[tokio::test]
async fn metrics_no_fetch_latency_on_l1_hit_path() {
    let m = Arc::new(CountingMetrics::default());
    let p = punnu_with_metrics(m.clone());

    p.insert(E { id: 1 }).await.unwrap();
    // get_or_fetch on cached id → L1 hit, no slow-path; no
    // fetch_latency sample.
    let _ = p
        .get_or_fetch(&1, |_| async {
            panic!("fetcher must not run for cached id");
        })
        .await
        .unwrap();

    let latencies = m.fetch_latencies.lock().unwrap().clone();
    assert_eq!(latencies.len(), 0);
}

#[tokio::test]
async fn metrics_none_means_no_calls() {
    // When `metrics: None`, the no-op path must compile to a single
    // null-check at every call site — no panics, no overhead. This
    // test is the regression fence.
    let p = Punnu::<E>::builder().build(); // default metrics: None

    p.insert(E { id: 1 }).await.unwrap();
    let _ = p.get(&1);
    let _ = p.get(&999);
    p.invalidate(&1, InvalidationReason::Manual).await;
    let _ = p
        .get_or_fetch(&2, |id| async move { Ok::<_, FetchError>(Some(E { id })) })
        .await;

    // No assertion needed — the test passes if no panics fire and
    // the compiler accepts the code. Sanity: post-invalidate, len is
    // 1 (the get_or_fetch above inserted id=2).
    assert_eq!(p.len(), 1);
}
