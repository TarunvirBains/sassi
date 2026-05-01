//! Task 6 — TTL lazy-expiry path.
//!
//! Spec §6.2.5: when an entry's `expires_at` deadline has passed,
//! `Punnu::get` returns `None`, removes the entry from L1, and emits
//! a `PunnuEvent::Invalidate { reason: TtlExpired }`. This file
//! exercises that contract — the background sweep variant lives in
//! `punnu_ttl_sweep.rs`.
//!
//! All tests run under `#[tokio::test(start_paused = true)]` and use
//! `tokio::time::advance(...)` to drive virtual time forward
//! deterministically. Sassi's TTL bookkeeping reads
//! `tokio::time::Instant::now()` (a drop-in for `std::time::Instant`
//! that honours the paused clock), so virtual-time advancement is
//! observable to the cache. No real `sleep` calls — the tests are
//! impervious to CI scheduling jitter.

use sassi::{Cacheable, EventReason, Field, Punnu, PunnuConfig, PunnuEvent};
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

    // Subscribe BEFORE the expiry triggers so we can observe the
    // TtlExpired event without racing.
    let mut rx = p.events();
    tokio::time::advance(Duration::from_secs(61)).await;

    assert!(
        p.get(&1).is_none(),
        "expired entry should be None after TTL elapses"
    );

    // The lazy-expiry path emits TtlExpired exactly once for the
    // observed entry.
    match rx.try_recv().expect("expected TtlExpired event") {
        PunnuEvent::Invalidate {
            id: 1,
            reason: EventReason::TtlExpired,
        } => {}
        other => panic!("expected TtlExpired for id=1, got {other:?}"),
    }
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
async fn lazy_expiry_decrements_len() {
    // After lazy expiry on get, the entry must be physically gone.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    assert_eq!(p.len(), 1);
    tokio::time::advance(Duration::from_secs(6)).await;
    let _ = p.get(&1); // triggers lazy expiry
    assert_eq!(p.len(), 0, "lazy expiry must remove the entry from L1");
}

#[tokio::test(start_paused = true)]
async fn lazy_expiry_event_fires_at_most_once_for_a_given_entry() {
    // Two `get`s after expiry should not produce two TtlExpired
    // events — the first `get` removes the entry; the second `get`
    // observes a miss with no event.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    let mut rx = p.events();
    tokio::time::advance(Duration::from_secs(6)).await;

    // First get: triggers lazy expiry, emits TtlExpired.
    assert!(p.get(&1).is_none());
    // Second get: cache miss, no event.
    assert!(p.get(&1).is_none());

    let mut ttl_events_observed = 0;
    while let Ok(ev) = rx.try_recv() {
        if matches!(
            ev,
            PunnuEvent::Invalidate {
                reason: EventReason::TtlExpired,
                ..
            }
        ) {
            ttl_events_observed += 1;
        }
    }
    assert_eq!(
        ttl_events_observed, 1,
        "TtlExpired must fire exactly once per expired entry, even with multiple gets"
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
async fn lazy_expiry_under_concurrent_gets_emits_one_event() {
    // Race condition probe: many concurrent `get` calls for the same
    // expired entry must collectively produce at most one
    // `TtlExpired` event. The expiry decision + pop happen under the
    // same write lock, so only the first `get` to acquire the lock
    // observes the entry and emits.
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
                reason: EventReason::TtlExpired,
                ..
            }
        ) {
            ttl_events_observed += 1;
        }
    }
    assert_eq!(
        ttl_events_observed, 1,
        "at most one TtlExpired event must fire across concurrent gets for an expired entry"
    );
}
