//! Task 5 — `Punnu<T>` event stream coverage.
//!
//! Exercises the four event-related contracts:
//!
//! - `insert` emits [`PunnuEvent::Insert`]
//! - `invalidate` emits [`PunnuEvent::Invalidate`] carrying the supplied
//!   [`InvalidationReason`]
//! - LRU eviction emits [`PunnuEvent::Invalidate { reason: LruEvict }`]
//! - The channel is **lossy under load** — a slow subscriber sees
//!   `RecvError::Lagged` rather than crashing the producer (spec §3.5
//!   lossy-by-design contract)
//!
//! Implementation lives in Task 4 (`pool.rs`); this file is the
//! observability test surface.

use sassi::{Cacheable, Field, InvalidationReason, OnConflict, Punnu, PunnuConfig, PunnuEvent};
use tokio::sync::broadcast::error::TryRecvError;

#[derive(Debug, Clone)]
struct E {
    id: i64,
    label: &'static str,
}

#[derive(Default)]
struct EFields {
    #[allow(dead_code)]
    id: Field<E, i64>,
    #[allow(dead_code)]
    label: Field<E, &'static str>,
}

impl Cacheable for E {
    type Id = i64;
    type Fields = EFields;
    fn id(&self) -> i64 {
        self.id
    }
}

#[tokio::test]
async fn insert_emits_insert_event() {
    let p = Punnu::<E>::builder().build();
    let mut rx = p.events();
    p.insert(E { id: 1, label: "a" }).await.unwrap();

    match rx.try_recv().expect("expected an Insert event") {
        PunnuEvent::Insert { value } => {
            assert_eq!(value.id, 1);
            assert_eq!(value.label, "a");
        }
        other => panic!("expected PunnuEvent::Insert, got {other:?}"),
    }
    // Stream should be drained — no spurious follow-up events.
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn replace_under_last_write_wins_emits_insert_not_update() {
    // Default OnConflict is LastWriteWins, which emits Insert (not
    // Update) on replace — Update is a separate opt-in policy.
    let p = Punnu::<E>::builder().build();
    p.insert(E { id: 1, label: "v1" }).await.unwrap();
    let mut rx = p.events();
    p.insert(E { id: 1, label: "v2" }).await.unwrap();

    match rx.try_recv().expect("expected Insert (LastWriteWins)") {
        PunnuEvent::Insert { value } => assert_eq!(value.label, "v2"),
        other => panic!("expected PunnuEvent::Insert, got {other:?}"),
    }
    // No invalidate event for the replaced entry — replace is a single
    // logical operation, not delete+insert.
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn replace_under_on_conflict_update_emits_update_event() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Update,
            ..Default::default()
        })
        .build();
    p.insert(E {
        id: 1,
        label: "old",
    })
    .await
    .unwrap();
    let mut rx = p.events();
    p.insert(E {
        id: 1,
        label: "new",
    })
    .await
    .unwrap();

    match rx.try_recv().expect("expected Update event") {
        PunnuEvent::Update { old, new } => {
            assert_eq!(old.label, "old");
            assert_eq!(new.label, "new");
        }
        other => panic!("expected PunnuEvent::Update, got {other:?}"),
    }
}

#[tokio::test]
async fn invalidate_emits_invalidate_with_reason() {
    let p = Punnu::<E>::builder().build();
    p.insert(E { id: 1, label: "a" }).await.unwrap();
    let mut rx = p.events();

    p.invalidate(&1, InvalidationReason::Manual).await;
    match rx.try_recv().expect("expected Invalidate event") {
        PunnuEvent::Invalidate { id, reason } => {
            assert_eq!(id, 1);
            assert_eq!(reason, InvalidationReason::Manual);
        }
        other => panic!("expected PunnuEvent::Invalidate, got {other:?}"),
    }
}

#[tokio::test]
async fn invalidate_unknown_id_emits_no_event() {
    // Idempotency: invalidating a missing id is a no-op — and a no-op
    // must not produce a phantom event (subscribers that count
    // invalidations would otherwise overcount).
    let p = Punnu::<E>::builder().build();
    let mut rx = p.events();
    p.invalidate(&42, InvalidationReason::Manual).await;
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn lru_evict_fires_invalidate_event() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let mut rx = p.events();
    p.insert(E { id: 1, label: "a" }).await.unwrap();
    p.insert(E { id: 2, label: "b" }).await.unwrap();

    // Drain everything we received and look for the eviction event.
    // Capacity-1 + two inserts must produce exactly one LruEvict for
    // id=1 plus two Insert events.
    let mut saw_evict = false;
    let mut saw_insert_1 = false;
    let mut saw_insert_2 = false;
    while let Ok(ev) = rx.try_recv() {
        match ev {
            PunnuEvent::Invalidate {
                id: 1,
                reason: InvalidationReason::LruEvict,
            } => saw_evict = true,
            PunnuEvent::Insert { value } if value.id == 1 => saw_insert_1 = true,
            PunnuEvent::Insert { value } if value.id == 2 => saw_insert_2 = true,
            other => panic!("unexpected event in lru-evict test: {other:?}"),
        }
    }
    assert!(saw_evict, "expected LruEvict event for id=1");
    assert!(saw_insert_1, "expected Insert event for id=1");
    assert!(saw_insert_2, "expected Insert event for id=2");
}

#[tokio::test]
async fn lru_evict_event_orders_before_insert_event_for_new_entry() {
    // Spec semantic: the eviction "happened first" to make room.
    // Subscribers that build distributed-cache invalidation streams
    // depend on this ordering.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1, label: "a" }).await.unwrap();
    let mut rx = p.events(); // subscribe AFTER id=1 so we don't see its insert
    p.insert(E { id: 2, label: "b" }).await.unwrap();

    let first = rx.try_recv().expect("first event");
    let second = rx.try_recv().expect("second event");
    assert!(
        matches!(
            first,
            PunnuEvent::Invalidate {
                reason: InvalidationReason::LruEvict,
                ..
            }
        ),
        "first event must be the LRU eviction; got {first:?}"
    );
    assert!(
        matches!(second, PunnuEvent::Insert { ref value } if value.id == 2),
        "second event must be the new insert; got {second:?}"
    );
}

#[tokio::test]
async fn lossy_when_subscriber_lags_does_not_crash_producer() {
    // Lossy-by-design: when a subscriber falls behind the channel
    // capacity, the broadcast channel drops the oldest events for
    // that subscriber and surfaces RecvError::Lagged. The producer
    // never blocks. This is the spec §3.5 lossy contract.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: 1024,
            event_channel_capacity: 4,
            ..Default::default()
        })
        .build();
    let mut rx = p.events();
    for i in 0..100 {
        // Producer must not block / panic / error even though the
        // subscriber never drains.
        p.insert(E { id: i, label: "x" }).await.unwrap();
    }

    // Drain whatever the subscriber sees; we must observe a Lagged
    // error at least once given the 4-cap channel and 100 events.
    let mut saw_lagged = false;
    let mut events_drained = 0;
    loop {
        match rx.try_recv() {
            Ok(_) => events_drained += 1,
            Err(TryRecvError::Lagged(_)) => {
                saw_lagged = true;
                // Continue draining — Lagged just signals "we skipped
                // forward in the queue"; receiver is still usable.
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Closed) => panic!("channel closed unexpectedly"),
        }
    }
    assert!(
        saw_lagged,
        "expected RecvError::Lagged given 100 events into a 4-capacity channel"
    );
    // Exact count is non-deterministic (depends on scheduling), but
    // the channel capacity is the upper bound on pending events.
    assert!(
        events_drained <= 4,
        "channel capacity is 4, so we cannot drain more than 4 buffered events at any moment; \
         drained {events_drained}"
    );
    // And the cache itself stayed healthy throughout.
    assert_eq!(p.len(), 100);
}

#[tokio::test]
async fn no_active_subscribers_does_not_break_insert() {
    // broadcast::Sender::send returns Err(SendError) when there are
    // zero active receivers. Punnu must swallow that gracefully —
    // events are observability, not a correctness boundary.
    let p = Punnu::<E>::builder().build();
    // No call to events() — zero subscribers.
    p.insert(E { id: 1, label: "a" }).await.unwrap();
    p.invalidate(&1, InvalidationReason::Manual).await;
    assert_eq!(p.len(), 0);
}
