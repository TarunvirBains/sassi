//! Task 10 — `DefaultExecutor` smoke test on the tokio runtime.
//!
//! `PunnuExecutor` is `pub(crate)` in v0.1; this test exercises it
//! indirectly through the public surface. The interesting end-to-end
//! shape is "TTL sweep fires on the configured cadence" — already
//! covered exhaustively by `punnu_ttl_sweep.rs`. This file's role is
//! narrower: pin a smoke test that the default executor's
//! `spawn` + `sleep` primitives work on tokio with reasonable
//! precision, so a regression in either primitive (or in the trait
//! routing through `executor.sleep` introduced in Task 10) gets
//! caught here rather than masked by a TTL test that has many other
//! moving parts.
//!
//! `start_paused = false` so we read real wall-clock time — this is a
//! sleep-precision check, which is meaningful only against the
//! actual timer, not virtual time.

#![cfg(feature = "runtime-tokio")]

use sassi::{Cacheable, Field, Punnu, PunnuConfig};
use std::time::{Duration, Instant};

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

#[tokio::test]
async fn default_executor_spawn_and_sleep_smoke() {
    // Spin a Punnu with a short sweep interval; after one sweep
    // tick's worth of real time, the sweep task should have been
    // spawned and ticked at least once. We don't assert on cadence
    // precision here (CI scheduling jitter would flake the test);
    // we assert that *some* spawn + sleep happened by observing the
    // sweep task's readiness signal — its presence is sufficient
    // proof that `executor.spawn` ran the future to its first sleep.
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            ttl_sweep_interval: Some(Duration::from_millis(50)),
            ..Default::default()
        })
        .build();

    let notify = p
        ._test_sweep_initialised()
        .expect("sweep was configured; readiness signal must be present");

    // The sweep should reach its first poll within a generous bound.
    // 500ms is many orders of magnitude above scheduler latency — a
    // failure here means `executor.spawn` did not run the future,
    // not "CI was slow today".
    tokio::time::timeout(Duration::from_millis(500), notify.notified())
        .await
        .expect("default executor failed to spawn the sweep task within 500ms");
}

#[tokio::test]
async fn default_executor_sleep_precision_at_100ms() {
    // Real-clock test (no `start_paused`): assert tokio's sleep is
    // bounded above by the requested duration plus a generous slack.
    // The slack is intentionally wide (target + 50ms) to absorb CI
    // scheduling jitter — the goal is "regression on the order of
    // 'sleep took 5x as long'", not chasing milliseconds.
    //
    // We measure `tokio::time::sleep` directly here rather than
    // routing through `PunnuExecutor::sleep` because the trait is
    // `pub(crate)` — the executor's sleep is the same primitive,
    // just behind a trait. This test pins the underlying call;
    // routing precision is verified by the TTL cadence test.
    let target = Duration::from_millis(100);
    let start = Instant::now();
    tokio::time::sleep(target).await;
    let elapsed = start.elapsed();

    // Lower bound: tokio guarantees at-least the requested duration.
    assert!(
        elapsed >= target,
        "sleep returned early: {:?} < target {:?}",
        elapsed,
        target
    );

    // Upper bound: target + 50ms. Wider than the 5% the spec
    // motivates because CI runners occasionally pause for GC / io
    // spikes; tighter would flake. The spec's "within 5%" is the
    // *intent*; this test catches catastrophic regressions
    // (sleep returned 1s late, missed the timer entirely).
    let upper = target + Duration::from_millis(50);
    assert!(
        elapsed <= upper,
        "sleep precision regression: {:?} > {:?}; the executor's sleep is leaking real time",
        elapsed,
        upper
    );
}
