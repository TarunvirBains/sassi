//! Task 6 — TTL background-sweep path.
//!
//! Spec §6.2.5: when [`PunnuConfig::ttl_sweep_interval`] is `Some`,
//! a background task scans the L1 every interval tick, removes
//! anything whose `expires_at` has elapsed, and emits
//! `PunnuEvent::Invalidate { reason: TtlExpired }` for each. This
//! file exercises that path; the lazy variant lives in
//! `punnu_ttl_lazy.rs`.
//!
//! The sweep is gated behind the `runtime-tokio` feature (it
//! depends on `tokio::spawn`); these tests use the default feature
//! set, which includes `runtime-tokio`.

use sassi::{Cacheable, Field, InvalidationReason, Punnu, PunnuConfig, PunnuEvent};
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
}

#[tokio::test]
async fn sweep_removes_expired_without_get() {
    // Without sweep, an expired entry stays in L1 storage until a
    // `get` triggers the lazy path. With sweep configured, it's
    // gone from `len` after at most one sweep interval after expiry.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_millis(50)),
            ttl_sweep_interval: Some(Duration::from_millis(20)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    assert_eq!(p.len(), 1);

    // Wait long enough for: TTL elapsed (50ms) + at least one sweep
    // tick after expiry (20ms) + the initial tick the sweep skips
    // (20ms) + slack. 200ms is generous and keeps the test stable
    // on slow CI.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        p.len(),
        0,
        "background sweep should have removed the expired entry"
    );
}

#[tokio::test]
async fn sweep_emits_ttl_expired_event() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_millis(50)),
            ttl_sweep_interval: Some(Duration::from_millis(20)),
            ..Default::default()
        })
        .build();
    let mut rx = p.events();
    p.insert(E { id: 7 }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain everything; we should observe an Insert + a TtlExpired
    // event for id 7.
    let mut saw_ttl = false;
    while let Ok(ev) = rx.try_recv() {
        if matches!(
            ev,
            PunnuEvent::Invalidate {
                id: 7,
                reason: InvalidationReason::TtlExpired,
            }
        ) {
            saw_ttl = true;
        }
    }
    assert!(saw_ttl, "background sweep must emit TtlExpired");
}

#[tokio::test]
async fn sweep_does_not_remove_unexpired_entries() {
    // An entry whose TTL hasn't elapsed must survive the sweep.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            // Long TTL on the entry, short sweep interval.
            default_ttl: Some(Duration::from_secs(5)),
            ttl_sweep_interval: Some(Duration::from_millis(20)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();

    // Several sweep ticks pass without the TTL elapsing.
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(p.len(), 1, "unexpired entry must survive the sweep");
    assert!(p.get(&1).is_some());
}

#[tokio::test]
async fn sweep_handles_mixed_expired_and_fresh() {
    // Insert two entries with different TTLs. The shorter expires;
    // the longer survives.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            ttl_sweep_interval: Some(Duration::from_millis(20)),
            ..Default::default()
        })
        .build();
    p.insert_with_ttl(E { id: 1 }, Duration::from_millis(40))
        .await
        .unwrap();
    p.insert_with_ttl(E { id: 2 }, Duration::from_secs(5))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(180)).await;

    assert!(
        p.get(&1).is_none(),
        "id 1 (40ms TTL) should have been swept"
    );
    assert!(p.get(&2).is_some(), "id 2 (5s TTL) should still be present");
}

#[tokio::test]
async fn sweep_terminates_when_punnu_dropped() {
    // Owner-loss cancellation: when every Punnu<T> clone is dropped,
    // the sweep's Weak::upgrade returns None on the next tick and
    // the task exits. This is structural rather than observable —
    // we verify that we can drop the pool without hangs and that
    // dropping doesn't panic. The strong indicator is that the
    // test process exits cleanly (no orphan tasks holding refs).
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            ttl_sweep_interval: Some(Duration::from_millis(10)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    drop(p);
    // Give the sweep one or two ticks to observe the drop and exit.
    tokio::time::sleep(Duration::from_millis(50)).await;
    // No assertion — the test is the absence of a hang or panic.
}

#[tokio::test]
async fn sweep_interval_off_means_no_background_removal() {
    // Default config has `ttl_sweep_interval = None`. An expired
    // entry stays in L1 (occupying capacity) until a `get` triggers
    // the lazy path. This matches the spec — sweep is opt-in.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_millis(40)),
            // ttl_sweep_interval explicitly None.
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        p.len(),
        1,
        "without ttl_sweep_interval, expired entries stay until next get"
    );

    // The lazy path then removes it.
    assert!(p.get(&1).is_none());
    assert_eq!(p.len(), 0);
}
