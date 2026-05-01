//! Storage entry layout + TTL helpers.
//!
//! The internal LRU map stores [`Entry<T>`] rather than `Arc<T>`
//! directly so per-entry metadata (currently just `expires_at`) lives
//! alongside the value. Consumers never see `Entry<T>` — `Punnu`
//! returns `Arc<T>` from `get` / `insert`. The storage shape
//! ([`Entry<T>`] with `expires_at: Option<Instant>`) lands alongside
//! the pool itself; the lazy-expiry behaviour and the background
//! sweep that act on `expires_at` live in this module too.
//!
//! # TTL semantics (spec §6.2.5)
//!
//! Two mechanisms enforce expiry:
//!
//! - **Lazy expiry on access.** Always active. [`Punnu::get`] checks
//!   `expires_at <= now`; if expired, removes the entry from L1 and
//!   emits `PunnuEvent::Invalidate { reason: TtlExpired }`. Cost: one
//!   comparison per `get`.
//! - **Background sweep.** Active iff
//!   [`crate::punnu::PunnuConfig::ttl_sweep_interval`] is `Some`. Walks
//!   the L1 every interval tick, removes anything already expired,
//!   emits `TtlExpired` for each. Bounded O(n) per tick where n is
//!   the entry count; the sweep takes the L1 write lock briefly. Off
//!   by default — only worth running when the access pattern leaves
//!   long-tail expired entries lingering in storage and the metrics
//!   layer or downstream subscribers rely on prompt removal.
//!
//! Interaction with other invalidation paths is documented in the
//! spec; the short version: independent of LRU and save/delete; same
//! reason discriminator (`TtlExpired`) regardless of which mechanism
//! observes the expiry first.
//!
//! # Why an internal struct rather than a tuple?
//!
//! - **Forward compatibility:** later metadata (per-entry `inserted_at`
//!   for refresh hints, per-entry `tenant_origin` for cross-tenant
//!   guard diagnostics, …) lands without changing every callsite.
//! - **Discoverability:** `entry.is_expired_at(now)` reads better than
//!   `entry.1.map(|t| t <= now).unwrap_or(false)` and keeps the
//!   comparison policy in one place.
//! - **Clone semantics:** `Entry<T>: Clone` cheaply because the
//!   payload is `Arc<T>`; `Instant` is `Copy`. Cloning happens
//!   on every `get` returning a sharable handle.

#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
use crate::cacheable::Cacheable;
#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
use crate::punnu::events::{EventReason, PunnuEvent};
#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
use crate::punnu::pool::PunnuInner;
use crate::time::Instant;
use std::sync::Arc;
#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
use std::sync::Weak;
#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
use tokio::sync::Notify;

/// LRU storage cell — holds the cached payload plus per-entry
/// metadata.
///
/// `pub(crate)` because `Punnu` returns `Arc<T>` from `get`; the
/// metadata is internal bookkeeping.
pub(crate) struct Entry<T> {
    /// Shared handle to the cached payload.
    pub value: Arc<T>,

    /// Absolute expiry deadline, computed from
    /// `executor.now() + ttl` at insert time. `None` means the entry
    /// never expires on time (LRU eviction can still drop it). The
    /// [`Instant`] type is target-aware (see [`crate::time`]) — on
    /// native it's `tokio::time::Instant` so test pause/advance is
    /// honoured; on wasm it's `web_time::Instant` (browser
    /// `Performance.now()`).
    pub expires_at: Option<Instant>,
}

impl<T> Entry<T> {
    /// Construct an entry with an explicit expiry deadline. `None`
    /// means the entry never expires on time (LRU eviction is the
    /// only way for it to leave the cache).
    pub(crate) fn with_expiry(value: Arc<T>, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }

    /// Has the entry's TTL elapsed against `now`?
    ///
    /// Pure (no clock read) so callers can sample a `now` once per
    /// sweep tick (or once per `get`) and reuse it across many
    /// entries — the background sweep does exactly this.
    pub(crate) fn is_expired_at(&self, now: Instant) -> bool {
        match self.expires_at {
            Some(deadline) => deadline <= now,
            None => false,
        }
    }
}

// Manual `Clone`: deriving would require `T: Clone` even though the
// only data field is `Arc<T>` (which is `Clone` regardless of `T`).
impl<T> Clone for Entry<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            expires_at: self.expires_at,
        }
    }
}

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
#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
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

            // Scope the lock so we drop it before broadcasting the
            // events — the broadcast send is non-blocking but should
            // still not extend lock-hold time.
            let expired_ids: Vec<T::Id> = {
                let mut map = match inner.map.write() {
                    Ok(guard) => guard,
                    // Lock poisoning means a previous panic left the
                    // map in an inconsistent state. The sweep can't
                    // do anything useful in that case; bail out. The
                    // public API surface returns the same poison.
                    Err(_) => break,
                };
                let now = inner.executor.now();
                let mut to_remove = Vec::new();
                for (id, entry) in map.iter() {
                    if entry.is_expired_at(now) {
                        to_remove.push(id.clone());
                    }
                }
                for id in &to_remove {
                    map.pop(id);
                }
                to_remove
            };

            // Record metrics outside the lock — `record_*` is a
            // consumer-defined trait method that may do arbitrary
            // work; we don't want it inside the L1 write-lock scope.
            // Spec §3.5.1: TTL-driven evictions count, the dashboard
            // splits by reason. We also sample `record_lru_size`
            // once per sweep tick that removed anything (no-op when
            // expired_ids is empty — the size didn't change).
            let removed_count = expired_ids.len();
            for id in expired_ids {
                let _ = inner.events.send(PunnuEvent::Invalidate {
                    id,
                    reason: EventReason::TtlExpired,
                });
                if let Some(m) = &inner.config.metrics {
                    m.record_eviction(std::any::type_name::<T>(), EventReason::TtlExpired);
                }
            }
            if removed_count > 0
                && let Some(m) = &inner.config.metrics
            {
                let post_len = match inner.map.read() {
                    Ok(g) => g.len(),
                    Err(_) => continue,
                };
                m.record_lru_size(std::any::type_name::<T>(), post_len);
            }
        }
    }));
}
