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
//! depends on `tokio::spawn`); this whole test file is gated the
//! same way so the no-default-features build (`cargo test
//! --no-default-features`) skips it cleanly. With the feature off,
//! `Punnu::insert` succeeds but the sweep task never spawns, so the
//! "sweep should have removed expired entry" assertions would fail
//! silently — the gate makes that contract explicit.
//!
//! All tests run under `#[tokio::test(start_paused = true)]` and use
//! `tokio::time::advance(...)` to drive virtual time forward
//! deterministically. Sassi's TTL bookkeeping reads
//! `tokio::time::Instant::now()`, which honours the paused clock, so
//! advancement is observable to the cache. Background sweep ticks
//! also fire under the paused clock — `tokio::time::interval` is
//! governed by the same virtual clock — so a `tokio::task::yield_now`
//! after `advance` is sufficient to let the sweep loop process its
//! pending tick(s). No real `sleep` calls; the tests are impervious
//! to CI scheduling jitter.

#![cfg(feature = "runtime-tokio")]

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

/// Anchor the background sweep task's interval timer at the current
/// virtual time.
///
/// `tokio::spawn(...)` queues the sweep's future but doesn't poll it
/// until the runtime yields; on a `current_thread` runtime under
/// `#[tokio::test(start_paused = true)]`, that means the sweep's
/// `tokio::time::interval` is constructed only after the test code
/// yields. If we `advance` before the sweep has been polled, the
/// interval anchors at the *advanced* time and the sweep wakes one
/// interval *after* the advance — the test then sees no removal.
///
/// Calling `prime_sweep` after `Punnu::builder().build()` and any
/// inserts forces the sweep to be polled at least once: it creates
/// its interval, awaits the immediate first tick (the sweep skips
/// it), and parks on the second tick. Subsequent `advance` calls
/// will then fire the registered timer.
///
/// **Heuristic, not a guarantee.** Tokio does not formally guarantee
/// scheduling order after [`tokio::task::yield_now`]; two yields
/// cover the common case (current_thread runtime + paused clock +
/// spawn-then-tick sequence) but a future scheduler change could
/// make this flake. A `tokio::sync::Notify`-based handshake from
/// the sweep task to the test would be deterministic; tracked for
/// the v0.2 testing-helper refactor in
/// <https://github.com/TarunvirBains/sassi/issues/4>. For v0.1.0,
/// these tests have been stable across CI runs and the heuristic
/// is acceptable.
async fn prime_sweep() {
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
}

/// Yield enough times for the background sweep loop to process its
/// pending wake-ups after a `tokio::time::advance`. The sweep task's
/// `tick.tick().await` is a wakeup point on the paused clock; one
/// yield per tick we want to process is sufficient because the sweep
/// does no awaits between ticks (it acquires the L1 lock
/// synchronously, drains, releases).
async fn drive_sweep_ticks(n: usize) {
    for _ in 0..n {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn sweep_removes_expired_without_get() {
    // Without sweep, an expired entry stays in L1 storage until a
    // `get` triggers the lazy path. With sweep configured, it's
    // gone from `len` after at most one sweep interval after expiry.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            ttl_sweep_interval: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    assert_eq!(p.len(), 1);

    // Pin the sweep's interval timer at virtual t=0 before advancing.
    prime_sweep().await;

    // Advance past TTL and several sweep ticks. The sweep skips the
    // first immediate tick (see `spawn_sweep`), so we need a couple
    // of ticks past the expiry to be sure the sweep observed it.
    tokio::time::advance(Duration::from_secs(10)).await;
    drive_sweep_ticks(4).await;

    assert_eq!(
        p.len(),
        0,
        "background sweep should have removed the expired entry"
    );
}

#[tokio::test(start_paused = true)]
async fn sweep_emits_ttl_expired_event() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            ttl_sweep_interval: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
    let mut rx = p.events();
    p.insert(E { id: 7 }).await.unwrap();

    prime_sweep().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    drive_sweep_ticks(4).await;

    // Drain everything; we should observe an Insert + a TtlExpired
    // event for id 7.
    let mut saw_ttl = false;
    while let Ok(ev) = rx.try_recv() {
        if matches!(
            ev,
            PunnuEvent::Invalidate {
                id: 7,
                reason: EventReason::TtlExpired { .. },
            }
        ) {
            saw_ttl = true;
        }
    }
    assert!(saw_ttl, "background sweep must emit TtlExpired");
}

#[tokio::test(start_paused = true)]
async fn sweep_does_not_remove_unexpired_entries() {
    // An entry whose TTL hasn't elapsed must survive the sweep.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            // Long TTL on the entry, short sweep interval.
            default_ttl: Some(Duration::from_secs(60 * 60)),
            ttl_sweep_interval: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();

    // Several sweep ticks pass without the TTL elapsing.
    prime_sweep().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    drive_sweep_ticks(6).await;
    assert_eq!(p.len(), 1, "unexpired entry must survive the sweep");
    assert!(p.get(&1).is_some());
}

#[tokio::test(start_paused = true)]
async fn sweep_handles_mixed_expired_and_fresh() {
    // Insert two entries with different TTLs. The shorter expires;
    // the longer survives.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            ttl_sweep_interval: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
    p.insert_with_ttl(E { id: 1 }, Duration::from_secs(5))
        .await
        .unwrap();
    p.insert_with_ttl(E { id: 2 }, Duration::from_secs(60 * 60))
        .await
        .unwrap();

    prime_sweep().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    drive_sweep_ticks(4).await;

    assert!(p.get(&1).is_none(), "id 1 (5s TTL) should have been swept");
    assert!(p.get(&2).is_some(), "id 2 (1h TTL) should still be present");
}

#[tokio::test(start_paused = true)]
async fn sweep_terminates_when_punnu_dropped() {
    // Owner-loss cancellation: when every Punnu<T> clone is dropped,
    // the sweep's Weak::upgrade returns None on the next tick and
    // the task exits. This is structural rather than observable —
    // we verify that we can drop the pool without hangs and that
    // dropping doesn't panic. The strong indicator is that the
    // test process exits cleanly (no orphan tasks holding refs).
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            ttl_sweep_interval: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();
    drop(p);
    // Give the sweep one or two ticks to observe the drop and exit.
    tokio::time::advance(Duration::from_secs(5)).await;
    drive_sweep_ticks(4).await;
    // No assertion — the test is the absence of a hang or panic.
}

#[tokio::test(start_paused = true)]
async fn sweep_removes_on_configured_tick_boundary() {
    // Cadence test — confirms removal happens on a sweep tick, not
    // before. With ttl_sweep_interval = 1s and default_ttl = 2s:
    // - At t=0: insert.
    // - Advance to t=1.5s (past first tick at t=1s, before TTL at t=2s):
    //   sweep ticks but entry is still fresh — nothing removed.
    // - Advance to t=2.5s (past TTL): entry is now expired but the
    //   sweep tick at t=2s already passed. The sweep at t=3s observes
    //   expiry and removes. Until then, len() == 1 (lazy path
    //   uninvoked since we don't call get).
    // - Drive ticks; verify removal.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(2)),
            ttl_sweep_interval: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();

    prime_sweep().await;

    // Just past the first sweep tick — entry not yet expired.
    tokio::time::advance(Duration::from_millis(1500)).await;
    drive_sweep_ticks(2).await;
    assert_eq!(
        p.len(),
        1,
        "sweep at t=1s ran but entry (TTL 2s) is still fresh"
    );

    // Cross the TTL deadline AND the next sweep tick.
    tokio::time::advance(Duration::from_millis(1500)).await;
    drive_sweep_ticks(2).await;
    assert_eq!(
        p.len(),
        0,
        "sweep at t=3s should have removed expired entry"
    );
}

#[tokio::test(start_paused = true)]
async fn sweep_interval_off_means_no_background_removal() {
    // Default config has `ttl_sweep_interval = None`. An expired
    // entry stays in L1 (occupying capacity) until a `get` triggers
    // the lazy path. This matches the spec — sweep is opt-in.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(5)),
            // ttl_sweep_interval explicitly None.
            ..Default::default()
        })
        .build();
    p.insert(E { id: 1 }).await.unwrap();

    tokio::time::advance(Duration::from_secs(10)).await;
    drive_sweep_ticks(4).await;
    assert_eq!(
        p.len(),
        1,
        "without ttl_sweep_interval, expired entries stay until next get"
    );

    // The lazy path then removes it.
    assert!(p.get(&1).is_none());
    assert_eq!(p.len(), 0);
}
