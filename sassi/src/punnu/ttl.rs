//! TTL background sweep helpers.
//!
//! # TTL semantics
//!
//! Two mechanisms enforce expiry:
//!
//! - **Lazy expiry on access.** Always active. [`Punnu::get`] checks
//!   `expires_at <= now`; if expired, returns `None` without cleanup
//!   or events. Cost: one comparison per `get`.
//! - **Background sweep.** Active iff
//!   [`crate::punnu::PunnuConfig::ttl_sweep_interval`] is `Some`.
//!   Walks the current L1 snapshot every interval tick to collect
//!   expired candidate ids, then rechecks those candidates under the
//!   write coordinator before publishing removals and emitting
//!   `TtlExpired` for each actually removed entry. Bounded O(n) per
//!   tick where n is the entry count. Off by default — only worth
//!   running when the access pattern leaves long-tail expired entries
//!   lingering in storage and the metrics layer or downstream
//!   subscribers rely on prompt removal.
//!
//! Interaction with other invalidation paths is documented in the
//! spec; the short version: sweep emits `TtlExpired`, while lazy reads
//! only record metrics and writers may remove expired entries silently
//! when they touch the same id or need to reclaim capacity.

#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
use crate::cacheable::Cacheable;
#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
use crate::punnu::pool::PunnuInner;
#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
use std::sync::Arc;
#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
use std::sync::Weak;
#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
use tokio::sync::Notify;

/// Spawn the background sweep task via the [`PunnuExecutor`]
/// abstraction.
///
/// Cancellation is handled via the [`Weak`] reference: when every
/// `Punnu<T>` clone drops, the strong count of `PunnuInner<T>` falls
/// to zero, `weak.upgrade()` returns `None`, and the loop exits
/// cleanly. No explicit handle, no `JoinHandle` to drop — the
/// cancellation primitive is owner-loss itself.
///
/// # Determinism handshake
///
/// `sweep_initialised` is fired exactly once on the sweep task's
/// first poll, *before* the first sleep. Tests can `await` that
/// signal before calling `tokio::time::advance(...)` — the sleep is
/// guaranteed to be registered against the test's virtual clock at
/// that point. Replaces the previous `tokio::task::yield_now`
/// heuristic; closes <https://github.com/TarunvirBains/sassi/issues/4>.
///
/// # Cross-target spawn
///
/// Calls `inner.executor.spawn(...)` — on native that's
/// `tokio::spawn`; on wasm it's `wasm_bindgen_futures::spawn_local`.
/// The sleep is similarly routed through `executor.sleep`. The sweep
/// body itself is runtime-agnostic.
#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
pub(crate) fn spawn_sweep<T: Cacheable>(
    weak: Weak<PunnuInner<T>>,
    interval: std::time::Duration,
    sweep_initialised: Arc<Notify>,
) {
    // Pre-spawn upgrade to capture the executor handle. If the inner
    // has already been dropped between `build()` and here, there is
    // nothing to sweep — return without spawning.
    let Some(inner_for_exec) = weak.upgrade() else {
        return;
    };
    let executor = inner_for_exec.executor.clone();
    drop(inner_for_exec);

    let exec_for_loop = executor.clone();
    executor.spawn(Box::pin(async move {
        // Fire the readiness signal on first poll, before any sleep.
        // This is the deterministic handshake that replaces the
        // yield-count heuristic — tests await this notification
        // before advancing virtual time, ensuring the first sleep is
        // registered against the test's clock.
        //
        // `notify_one()` (not `notify_waiters()`) so the signal is
        // race-free: if the test calls `_test_sweep_initialised`
        // *after* the sweep task has already ticked through this
        // line, the stored permit is consumed by the next
        // `notified()` call. `notify_waiters()` would silently drop
        // the wake-up in that ordering. Only one waiter consumes it
        // — that matches the test expectation (one prime_sweep call
        // per Punnu).
        sweep_initialised.notify_one();

        loop {
            exec_for_loop.sleep(interval).await;

            // Upgrade once per tick. If every Punnu<T> clone has
            // dropped, the inner is gone and we exit cleanly.
            let Some(inner) = weak.upgrade() else { break };

            let now = inner.executor.now();
            let expired_ids = {
                let snapshot = inner.l1.load_full();
                snapshot
                    .entries
                    .iter()
                    .filter(|(_, entry)| entry.is_expired_at(now))
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>()
            };

            #[cfg(all(
                test,
                any(
                    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
                    all(feature = "runtime-wasm", target_arch = "wasm32"),
                )
            ))]
            inner
                .pause_next_sweep_after_candidates_for_test(!expired_ids.is_empty())
                .await;

            if expired_ids.is_empty() {
                continue;
            }

            if !inner.sweep_expired(expired_ids) {
                break;
            }
        }
    }));
}
