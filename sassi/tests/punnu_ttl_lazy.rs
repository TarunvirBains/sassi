//! TTL lazy-expiry path.
//!
//! Spec §6.2.5: when an entry's `expires_at` deadline has passed,
//! `Punnu::get` returns `None` but does not mutate L1 and does not
//! emit an event. Writers treat expired entries as absent on their
//! next snapshot write. This file exercises that lazy contract — the
//! background sweep variant lives in `punnu_ttl_sweep.rs`.
//!
//! All tests run under `#[tokio::test(start_paused = true)]` and use
//! `tokio::time::advance(...)` to drive virtual time forward
//! deterministically. Sassi's TTL bookkeeping reads
//! `tokio::time::Instant::now()` (a drop-in for `std::time::Instant`
//! that honours the paused clock), so virtual-time advancement is
//! observable to the cache. No real `sleep` calls — the tests are
//! impervious to CI scheduling jitter.

use sassi::{Cacheable, EventReason, Field, OnConflict, Punnu, PunnuConfig, PunnuEvent};
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

#[tokio::test(start_paused = true)]
async fn ttl_expires_lazily_on_get() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    assert!(
        p.get(&1).is_some(),
        "entry visible immediately after insert"
    );

    let mut rx = p.events();
    tokio::time::advance(Duration::from_secs(61)).await;

    assert!(
        p.get(&1).is_none(),
        "expired entry should be None after TTL elapses"
    );
    assert_eq!(
        p.len(),
        1,
        "lazy TTL miss should not physically remove the expired entry"
    );
    assert!(
        matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "lazy TTL miss should not emit an event"
    );
}

#[tokio::test(start_paused = true)]
async fn no_default_ttl_means_no_expiry() {
    // With `default_ttl = None`, entries should never expire on time.
    let p = Punnu::<E>::builder().build();
    p.insert(E { id: 1 }).await.unwrap();
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(p.get(&1).is_some(), "no TTL configured — entry must remain");
}

#[tokio::test(start_paused = true)]
async fn insert_with_ttl_overrides_config_default() {
    // Per-entry override: even though the pool default is 60s,
    // the explicit insert_with_ttl wins for this entry.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .build();
    p.insert_with_ttl(E { id: 1 }, Duration::from_secs(5))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(
        p.get(&1).is_none(),
        "entry-level TTL must override pool default"
    );
}

#[tokio::test(start_paused = true)]
async fn insert_with_ttl_overrides_when_pool_default_is_none() {
    // Per-entry TTL works even when the pool has no default.
    let p = Punnu::<E>::builder().build();
    p.insert_with_ttl(E { id: 1 }, Duration::from_secs(5))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(p.get(&1).is_none());
}

#[tokio::test(start_paused = true)]
async fn insert_with_ttl_duration_max_disables_expiry_without_overflow() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
    p.insert_with_ttl(E { id: 1 }, Duration::MAX).await.unwrap();

    tokio::time::advance(Duration::from_secs(60 * 60)).await;
    assert!(
        p.get(&1).is_some(),
        "Duration::MAX should saturate to a non-expiring entry instead of overflowing"
    );
}

#[tokio::test(start_paused = true)]
async fn lazy_expiry_keeps_l1_entry_until_a_writer_replaces_it() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    assert_eq!(p.len(), 1);
    tokio::time::advance(Duration::from_secs(6)).await;
    let _ = p.get(&1);
    assert_eq!(
        p.len(),
        1,
        "lazy expiry should leave physical cleanup to a same-id writer, capacity pressure, or sweep"
    );

    p.insert(E { id: 1 }).await.unwrap();
    assert!(p.get(&1).is_some());
    assert_eq!(p.len(), 1, "replacement must not duplicate the key");
}

#[tokio::test(start_paused = true)]
async fn unrelated_writer_does_not_sweep_expired_entries_without_capacity_pressure() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            lru_size: 16,
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(p.get(&1).is_none());
    assert_eq!(p.len(), 1);

    p.insert(E { id: 2 }).await.unwrap();
    assert_eq!(
        p.len(),
        2,
        "unrelated writes should not perform an O(n) expired-entry sweep when capacity is not under pressure"
    );
    assert!(p.get(&2).is_some());
}

#[tokio::test(start_paused = true)]
async fn capacity_pressure_reclaims_expired_entries_before_lru_eviction() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            lru_size: 2,
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    p.insert_with_ttl(E { id: 2 }, Duration::from_secs(30))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(p.get(&1).is_none());
    assert!(p.get(&2).is_some());

    p.insert(E { id: 3 }).await.unwrap();
    assert_eq!(p.len(), 2);
    assert!(p.get(&1).is_none());
    assert!(
        p.get(&2).is_some(),
        "fresh entries must not be LRU-evicted while expired entries can satisfy capacity pressure"
    );
    assert!(p.get(&3).is_some());
}

#[tokio::test(start_paused = true)]
async fn lazy_expiry_get_emits_no_ttl_events() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    let mut rx = p.events();
    tokio::time::advance(Duration::from_secs(6)).await;

    assert!(p.get(&1).is_none());
    assert!(p.get(&1).is_none());

    let mut ttl_events_observed = 0;
    while let Ok(ev) = rx.try_recv() {
        if matches!(
            ev,
            PunnuEvent::Invalidate {
                reason: EventReason::TtlExpired { .. },
                ..
            }
        ) {
            ttl_events_observed += 1;
        }
    }
    assert_eq!(
        ttl_events_observed, 0,
        "lazy get should not emit TtlExpired events"
    );
}

#[tokio::test(start_paused = true)]
async fn unexpired_get_does_not_emit_ttl_event() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    let mut rx = p.events();

    // Get before TTL elapses — no event should fire.
    assert!(p.get(&1).is_some());
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test(start_paused = true)]
async fn lazy_expiry_under_concurrent_gets_emits_no_event_and_keeps_len() {
    // Many concurrent `get` calls for the same expired entry should
    // all miss without mutating L1 or emitting events.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    let mut rx = p.events();

    tokio::time::advance(Duration::from_secs(6)).await;

    // Fan out 16 concurrent `get`s. Each runs sync; tokio just
    // schedules them on the runtime.
    let mut handles = Vec::new();
    for _ in 0..16 {
        let p = p.clone();
        handles.push(tokio::spawn(async move { p.get(&1) }));
    }
    for h in handles {
        let v = h.await.unwrap();
        assert!(v.is_none(), "all concurrent gets must observe miss");
    }

    let mut ttl_events_observed = 0;
    while let Ok(ev) = rx.try_recv() {
        if matches!(
            ev,
            PunnuEvent::Invalidate {
                reason: EventReason::TtlExpired { .. },
                ..
            }
        ) {
            ttl_events_observed += 1;
        }
    }
    assert_eq!(
        ttl_events_observed, 0,
        "lazy get should not emit TtlExpired events under contention"
    );
    assert_eq!(p.len(), 1, "lazy get should not remove the expired entry");
}

#[tokio::test(start_paused = true)]
async fn insert_treats_expired_existing_entry_as_absent_under_reject() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            on_conflict: OnConflict::Reject,
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;

    let replaced = p
        .insert(E { id: 1 })
        .await
        .expect("expired entries should not cause Reject conflicts");

    assert_eq!(replaced.id, 1);
    assert_eq!(p.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn replacing_expired_entry_emits_insert_not_update() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            on_conflict: OnConflict::Update,
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    let mut rx = p.events();

    p.insert(E { id: 1 }).await.unwrap();

    match rx.try_recv().expect("expected insert event") {
        PunnuEvent::Insert { value } => assert_eq!(value.id, 1),
        PunnuEvent::Update { .. } => {
            panic!("replacing an expired entry should emit Insert, not Update")
        }
        other => panic!("expected Insert for expired replacement, got {other:?}"),
    }
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}
