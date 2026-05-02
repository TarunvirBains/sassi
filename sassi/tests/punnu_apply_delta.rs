//! `Punnu::apply_delta` atomic items+tombstones commit coverage.

use sassi::{
    Cacheable, DeltaResult, EventReason, Field, OnConflict, Punnu, PunnuConfig, PunnuEvent,
};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock, mpsc};
use std::thread::ThreadId;
use std::time::Duration;
use tokio::sync::broadcast::error::TryRecvError;

#[derive(Debug, Clone)]
struct Item {
    id: i64,
    group: &'static str,
    name: &'static str,
}

#[derive(Default)]
struct ItemFields {
    #[allow(dead_code)]
    id: Field<Item, i64>,
    #[allow(dead_code)]
    group: Field<Item, &'static str>,
}

impl Cacheable for Item {
    type Id = i64;
    type Fields = ItemFields;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {
        ItemFields {
            id: Field::new("id", |item| &item.id),
            group: Field::new("group", |item| &item.group),
        }
    }
}

fn delta(items: Vec<Item>, tombstones: impl IntoIterator<Item = i64>) -> DeltaResult<Item> {
    DeltaResult::new(items, tombstones.into_iter().collect())
}

#[tokio::test]
async fn apply_delta_tombstone_wins_over_item_for_same_id() {
    let p = Punnu::<Item>::builder().build();
    p.insert(Item {
        id: 1,
        group: "a",
        name: "old",
    })
    .await
    .unwrap();
    let mut rx = p.events();

    let stats = p.apply_delta(delta(
        vec![Item {
            id: 1,
            group: "a",
            name: "new",
        }],
        [1],
    ));

    assert_eq!(stats.applied_items, 0);
    assert_eq!(stats.tombstones_evicted, 1);
    assert_eq!(stats.lru_evictions, 0);
    assert!(p.get(&1).is_none());
    assert_eq!(p.len(), 0);

    match rx.try_recv().expect("expected tombstone event") {
        PunnuEvent::Invalidate {
            id: 1,
            reason: EventReason::OnDelete,
        } => {}
        other => panic!("expected OnDelete invalidation, got {other:?}"),
    }
    assert!(
        matches!(rx.try_recv(), Err(TryRecvError::Empty)),
        "tombstoned id must not emit Insert or Update"
    );
}

#[tokio::test]
async fn apply_delta_absence_from_items_does_not_delete_resident_entries() {
    let p = Punnu::<Item>::builder().build();
    p.insert(Item {
        id: 1,
        group: "a",
        name: "old-a",
    })
    .await
    .unwrap();
    p.insert(Item {
        id: 2,
        group: "b",
        name: "old-b",
    })
    .await
    .unwrap();

    let stats = p.apply_delta(delta(
        vec![Item {
            id: 1,
            group: "a",
            name: "new-a",
        }],
        [],
    ));

    assert_eq!(stats.applied_items, 1);
    assert_eq!(p.get(&1).unwrap().name, "new-a");
    assert_eq!(
        p.get(&2).unwrap().name,
        "old-b",
        "absence from one query delta must not delete another resident entry"
    );

    let stats = p.apply_delta(delta(vec![], [2]));
    assert_eq!(stats.tombstones_evicted, 1);
    assert!(p.get(&2).is_none(), "true tombstones delete globally");
}

#[tokio::test]
async fn apply_delta_filter_departure_omission_preserves_other_query_results() {
    let p = Punnu::<Item>::builder().build();
    p.insert(Item {
        id: 1,
        group: "a",
        name: "query-a-old",
    })
    .await
    .unwrap();
    p.insert(Item {
        id: 2,
        group: "b",
        name: "query-b",
    })
    .await
    .unwrap();

    let query_b_ids_before = p
        .scope(Vec::new())
        .filter_basic(|fields| fields.group.eq("b"))
        .collect()
        .into_iter()
        .map(|item| item.id)
        .collect::<Vec<_>>();
    assert_eq!(query_b_ids_before, vec![2]);

    let stats = p.apply_delta(delta(
        vec![Item {
            id: 1,
            group: "a",
            name: "query-a-new",
        }],
        [],
    ));

    assert_eq!(stats.applied_items, 1);
    let query_b_ids_after_omission = p
        .scope(Vec::new())
        .filter_basic(|fields| fields.group.eq("b"))
        .collect()
        .into_iter()
        .map(|item| item.id)
        .collect::<Vec<_>>();
    assert_eq!(
        query_b_ids_after_omission,
        vec![2],
        "omission from one filtered delta must not delete another query's resident entry"
    );

    let stats = p.apply_delta(delta(vec![], [2]));

    assert_eq!(stats.tombstones_evicted, 1);
    assert!(p.get(&2).is_none());
    assert!(
        p.scope(Vec::new())
            .filter_basic(|fields| fields.group.eq("b"))
            .collect()
            .is_empty(),
        "true tombstones remove the id from every filtered view"
    );
}

#[tokio::test]
async fn apply_delta_reject_policy_skips_live_prior_without_counting_item() {
    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Reject,
            ..Default::default()
        })
        .build();
    p.insert(Item {
        id: 1,
        group: "a",
        name: "old",
    })
    .await
    .unwrap();
    let mut rx = p.events();

    let stats = p.apply_delta(delta(
        vec![Item {
            id: 1,
            group: "a",
            name: "new",
        }],
        [],
    ));

    assert_eq!(stats.applied_items, 0);
    assert_eq!(p.get(&1).unwrap().name, "old");
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn apply_delta_duplicate_ids_last_write_wins_emits_one_final_insert() {
    let p = Punnu::<Item>::builder().build();
    let mut rx = p.events();

    let stats = p.apply_delta(delta(
        vec![
            Item {
                id: 1,
                group: "a",
                name: "first",
            },
            Item {
                id: 1,
                group: "a",
                name: "last",
            },
        ],
        [],
    ));

    assert_eq!(stats.applied_items, 1);
    assert_eq!(p.get(&1).unwrap().name, "last");
    match rx.try_recv().expect("expected one final insert") {
        PunnuEvent::Insert { value } => assert_eq!(value.name, "last"),
        other => panic!("expected Insert, got {other:?}"),
    }
    assert!(
        matches!(rx.try_recv(), Err(TryRecvError::Empty)),
        "duplicate ids must not emit events for transient values"
    );
}

#[tokio::test]
async fn apply_delta_duplicate_ids_update_reports_original_old_once() {
    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Update,
            ..Default::default()
        })
        .build();
    p.insert(Item {
        id: 1,
        group: "a",
        name: "old",
    })
    .await
    .unwrap();
    let mut rx = p.events();

    let stats = p.apply_delta(delta(
        vec![
            Item {
                id: 1,
                group: "a",
                name: "first",
            },
            Item {
                id: 1,
                group: "a",
                name: "last",
            },
        ],
        [],
    ));

    assert_eq!(stats.applied_items, 1);
    assert_eq!(p.get(&1).unwrap().name, "last");
    match rx.try_recv().expect("expected one final update") {
        PunnuEvent::Update { old, new } => {
            assert_eq!(old.name, "old");
            assert_eq!(new.name, "last");
        }
        other => panic!("expected Update, got {other:?}"),
    }
    assert!(
        matches!(rx.try_recv(), Err(TryRecvError::Empty)),
        "duplicate ids must not emit updates against transient old values"
    );
}

#[tokio::test]
async fn apply_delta_duplicate_ids_reject_conflicts_only_with_live_prior() {
    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Reject,
            ..Default::default()
        })
        .build();
    let mut rx = p.events();

    let stats = p.apply_delta(delta(
        vec![
            Item {
                id: 1,
                group: "a",
                name: "first",
            },
            Item {
                id: 1,
                group: "a",
                name: "last",
            },
        ],
        [],
    ));

    assert_eq!(stats.applied_items, 1);
    assert_eq!(p.get(&1).unwrap().name, "last");
    match rx.try_recv().expect("expected one final insert") {
        PunnuEvent::Insert { value } => assert_eq!(value.name, "last"),
        other => panic!("expected Insert, got {other:?}"),
    }
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test(start_paused = true)]
async fn apply_delta_treats_expired_prior_as_absent() {
    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            on_conflict: OnConflict::Update,
            ..Default::default()
        })
        .build();
    p.insert(Item {
        id: 1,
        group: "a",
        name: "old",
    })
    .await
    .unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    let mut rx = p.events();

    let stats = p.apply_delta(delta(
        vec![Item {
            id: 1,
            group: "a",
            name: "new",
        }],
        [],
    ));

    assert_eq!(stats.applied_items, 1);
    assert_eq!(p.get(&1).unwrap().name, "new");
    match rx.try_recv().expect("expected fresh insert event") {
        PunnuEvent::Insert { value } => assert_eq!(value.name, "new"),
        PunnuEvent::Update { .. } => {
            panic!("expired prior must be absent and emit Insert, not Update")
        }
        other => panic!("expected Insert event, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn apply_delta_tombstone_treats_expired_resident_as_absent() {
    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .build();
    p.insert(Item {
        id: 1,
        group: "a",
        name: "old",
    })
    .await
    .unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    let mut rx = p.events();

    let stats = p.apply_delta(delta(vec![], [1]));

    assert_eq!(stats.applied_items, 0);
    assert_eq!(
        stats.tombstones_evicted, 0,
        "expired residents are absent, not live tombstone removals"
    );
    assert_eq!(stats.lru_evictions, 0);
    assert_eq!(p.len(), 0);
    assert!(p.get(&1).is_none());
    assert!(
        matches!(rx.try_recv(), Err(TryRecvError::Empty)),
        "expired residents must not emit OnDelete tombstone events"
    );
}

#[tokio::test]
async fn apply_delta_reports_lru_evictions_after_delta() {
    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 2,
            ..Default::default()
        })
        .build();
    p.insert(Item {
        id: 1,
        group: "a",
        name: "one",
    })
    .await
    .unwrap();
    p.insert(Item {
        id: 2,
        group: "a",
        name: "two",
    })
    .await
    .unwrap();
    let mut rx = p.events();

    let stats = p.apply_delta(delta(
        vec![Item {
            id: 3,
            group: "a",
            name: "three",
        }],
        [],
    ));

    assert_eq!(stats.applied_items, 1);
    assert_eq!(stats.tombstones_evicted, 0);
    assert_eq!(stats.lru_evictions, 1);
    assert!(p.len() <= 2);
    assert!(p.get(&3).is_some());

    let mut saw_lru = false;
    while let Ok(event) = rx.try_recv() {
        if matches!(
            event,
            PunnuEvent::Invalidate {
                reason: EventReason::LruEvict { .. },
                ..
            }
        ) {
            saw_lru = true;
        }
    }
    assert!(saw_lru, "capacity overflow should emit an LRU event");
}

#[tokio::test]
async fn apply_delta_lru_event_orders_before_final_insert() {
    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    p.insert(Item {
        id: 1,
        group: "a",
        name: "old",
    })
    .await
    .unwrap();
    let mut rx = p.events();

    let stats = p.apply_delta(delta(
        vec![Item {
            id: 2,
            group: "a",
            name: "new",
        }],
        [],
    ));

    assert_eq!(stats.applied_items, 1);
    assert_eq!(stats.lru_evictions, 1);
    let first = rx.try_recv().expect("first event");
    let second = rx.try_recv().expect("second event");
    assert!(
        matches!(
            first,
            PunnuEvent::Invalidate {
                id: 1,
                reason: EventReason::LruEvict { .. },
            }
        ),
        "first event must be the LRU eviction; got {first:?}"
    );
    assert!(
        matches!(second, PunnuEvent::Insert { ref value } if value.id == 2),
        "second event must be the final insert; got {second:?}"
    );
}

#[tokio::test]
async fn apply_delta_reports_only_final_insert_events_when_delta_exceeds_capacity() {
    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 2,
            ..Default::default()
        })
        .build();
    let mut rx = p.events();

    let stats = p.apply_delta(delta(
        vec![
            Item {
                id: 1,
                group: "a",
                name: "one",
            },
            Item {
                id: 2,
                group: "a",
                name: "two",
            },
            Item {
                id: 3,
                group: "a",
                name: "three",
            },
            Item {
                id: 4,
                group: "a",
                name: "four",
            },
        ],
        [],
    ));

    let mut final_ids = p
        .scope(Vec::new())
        .collect()
        .into_iter()
        .map(|item| item.id)
        .collect::<Vec<_>>();
    final_ids.sort_unstable();
    assert_eq!(p.len(), 2);
    assert_eq!(stats.applied_items, final_ids.len());
    assert_eq!(
        stats.lru_evictions, 0,
        "LRU stats count previously visible resident IDs, not transient delta candidates"
    );

    let mut inserted_ids = Vec::new();
    while let Ok(event) = rx.try_recv() {
        match event {
            PunnuEvent::Insert { value } => inserted_ids.push(value.id),
            PunnuEvent::Invalidate {
                reason: EventReason::LruEvict { .. },
                ..
            } => panic!("transient delta candidates must not emit LRU invalidations"),
            other => panic!("unexpected event: {other:?}"),
        }
    }
    inserted_ids.sort_unstable();
    assert_eq!(
        inserted_ids, final_ids,
        "insert events must describe only values present in the committed snapshot"
    );
}

static BLOCK_VALUE: AtomicI64 = AtomicI64::new(i64::MIN);
static BLOCK_ACTIVE: AtomicBool = AtomicBool::new(false);
static HASH_BLOCKER: OnceLock<HashBlocker> = OnceLock::new();

struct HashBlocker {
    entered: (Mutex<bool>, Condvar),
    released: (Mutex<bool>, Condvar),
    owner: Mutex<Option<ThreadId>>,
}

impl HashBlocker {
    fn global() -> &'static Self {
        HASH_BLOCKER.get_or_init(|| Self {
            entered: (Mutex::new(false), Condvar::new()),
            released: (Mutex::new(false), Condvar::new()),
            owner: Mutex::new(None),
        })
    }

    fn arm(value: i64) {
        let blocker = Self::global();
        *blocker.entered.0.lock().unwrap() = false;
        *blocker.released.0.lock().unwrap() = false;
        *blocker.owner.lock().unwrap() = Some(std::thread::current().id());
        BLOCK_VALUE.store(value, Ordering::SeqCst);
        BLOCK_ACTIVE.store(true, Ordering::SeqCst);
    }

    fn wait_until_entered() {
        let blocker = Self::global();
        let mut entered = blocker.entered.0.lock().unwrap();
        while !*entered {
            entered = blocker.entered.1.wait(entered).unwrap();
        }
    }

    fn release() {
        let blocker = Self::global();
        BLOCK_ACTIVE.store(false, Ordering::SeqCst);
        let mut released = blocker.released.0.lock().unwrap();
        *released = true;
        blocker.released.1.notify_all();
    }

    fn block_here() {
        let blocker = Self::global();
        {
            let mut entered = blocker.entered.0.lock().unwrap();
            *entered = true;
            blocker.entered.1.notify_all();
        }

        let mut released = blocker.released.0.lock().unwrap();
        while !*released {
            released = blocker.released.1.wait(released).unwrap();
        }
    }

    fn current_thread_owns_blocker() -> bool {
        *Self::global().owner.lock().unwrap() == Some(std::thread::current().id())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BlockingId(i64);

impl Hash for BlockingId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if BLOCK_ACTIVE.load(Ordering::SeqCst)
            && self.0 == BLOCK_VALUE.load(Ordering::SeqCst)
            && HashBlocker::current_thread_owns_blocker()
        {
            HashBlocker::block_here();
        }
        self.0.hash(state);
    }
}

#[derive(Debug, Clone)]
struct BlockingItem {
    id: BlockingId,
    name: &'static str,
}

#[derive(Default)]
struct BlockingFields;

impl Cacheable for BlockingItem {
    type Id = BlockingId;
    type Fields = BlockingFields;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {
        BlockingFields
    }
}

#[test]
fn apply_delta_readers_observe_old_or_new_state_without_partial_publish() {
    let p = Punnu::<BlockingItem>::builder().build();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(p.insert(BlockingItem {
            id: BlockingId(1),
            name: "old",
        }))
        .unwrap();

    let writer_punnu = p.clone();
    let writer = std::thread::spawn(move || {
        HashBlocker::arm(3);
        let stats = writer_punnu.apply_delta(DeltaResult::new(
            vec![BlockingItem {
                id: BlockingId(3),
                name: "new",
            }],
            HashSet::from([BlockingId(1)]),
        ));
        assert_eq!(stats.applied_items, 1);
        assert_eq!(stats.tombstones_evicted, 1);
    });

    HashBlocker::wait_until_entered();

    let (tx, rx) = mpsc::channel();
    let reader_punnu = p.clone();
    let reader = std::thread::spawn(move || {
        let old = reader_punnu.get(&BlockingId(1)).map(|item| item.name);
        let new = reader_punnu.get(&BlockingId(3)).map(|item| item.name);
        tx.send((old, new)).unwrap();
    });

    let observed = rx.recv_timeout(Duration::from_millis(100));
    HashBlocker::release();
    writer.join().unwrap();
    reader.join().unwrap();

    assert_eq!(
        observed.expect("reader must not wait for delta prepare"),
        (Some("old"), None),
        "reader should observe the old committed snapshot while delta prepare is in flight"
    );
    assert!(p.get(&BlockingId(1)).is_none());
    assert_eq!(p.get(&BlockingId(3)).unwrap().name, "new");
}
