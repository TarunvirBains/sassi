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
//!   `expires_at <= Instant::now()`; if expired, removes the entry
//!   from L1 and emits `PunnuEvent::Invalidate { reason: TtlExpired
//!   }`. Cost: one comparison per `get`.
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
//! - **Discoverability:** `entry.is_expired()` reads better than
//!   `entry.1.map(|t| t <= Instant::now()).unwrap_or(false)` and keeps
//!   the comparison policy in one place.
//! - **Clone semantics:** `Entry<T>: Clone` cheaply because the
//!   payload is `Arc<T>`; `Instant` is `Copy`. Cloning happens
//!   on every `get` returning a sharable handle.

#[cfg(feature = "runtime-tokio")]
use crate::cacheable::Cacheable;
#[cfg(feature = "runtime-tokio")]
use crate::punnu::events::{EventReason, PunnuEvent};
#[cfg(feature = "runtime-tokio")]
use crate::punnu::pool::PunnuInner;
use std::sync::Arc;
#[cfg(feature = "runtime-tokio")]
use std::sync::Weak;
// `tokio::time::Instant` is a drop-in for `std::time::Instant` that
// honours `tokio::time::pause()` / `advance()` in tests. Production
// behaviour is identical (wall-clock semantics); test code that opts
// into `#[tokio::test(start_paused = true)]` gets deterministic
// virtual-time control over sassi's TTL bookkeeping. Tokio's `time`
// feature is unconditionally enabled in the workspace, so this import
// works for both `runtime-tokio` and the no-default-features build.
use tokio::time::Instant;

/// LRU storage cell — holds the cached payload plus per-entry
/// metadata.
///
/// `pub(crate)` because `Punnu` returns `Arc<T>` from `get`; the
/// metadata is internal bookkeeping.
pub(crate) struct Entry<T> {
    /// Shared handle to the cached payload.
    pub value: Arc<T>,

    /// Absolute expiry deadline, computed from
    /// `tokio::time::Instant::now() + ttl` at insert time. `None`
    /// means the entry never expires on time (LRU eviction can still
    /// drop it). `tokio::time::Instant` is a drop-in for
    /// `std::time::Instant` that honours `tokio::time::pause` in
    /// tests; production wall-clock semantics are unchanged.
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
    /// Pure (no clock read) so callers can sample `Instant::now()`
    /// once per sweep tick and reuse it across many entries — the
    /// background sweep does exactly this.
    pub(crate) fn is_expired_at(&self, now: Instant) -> bool {
        match self.expires_at {
            Some(deadline) => deadline <= now,
            None => false,
        }
    }

    /// Has the entry's TTL elapsed against the current clock?
    /// Convenience wrapper around `is_expired_at(Instant::now())` for
    /// the lazy-expiry path on `get`, where there's no batched clock
    /// to reuse.
    pub(crate) fn is_expired(&self) -> bool {
        self.is_expired_at(Instant::now())
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

/// Spawn the background sweep task on the tokio runtime.
///
/// Cancellation is handled via the [`Weak`] reference: when every
/// `Punnu<T>` clone drops, the strong count of `PunnuInner<T>` falls
/// to zero, `weak.upgrade()` returns `None`, and the loop exits
/// cleanly. No explicit handle, no `JoinHandle` to drop, no
/// `Notify` — the cancellation primitive is owner-loss itself.
///
/// Direct `tokio::spawn` here is wasm-incompatible; refactored
/// through `PunnuExecutor` in Cluster B, Task 10. WASM consumers
/// that opt in to TTL with `ttl_sweep_interval = Some(_)` need that
/// executor abstraction. Tracked at
/// <https://github.com/TarunvirBains/sassi/issues/3>.
#[cfg(feature = "runtime-tokio")]
pub(crate) fn spawn_sweep<T: Cacheable>(weak: Weak<PunnuInner<T>>, interval: std::time::Duration) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        // Skip the first immediate tick — `interval` fires once at t=0
        // by default, which would sweep before any insert lands.
        // `Burst` is the default `MissedTickBehavior` and is fine
        // here; we just prefer to start the cadence at +interval.
        tick.tick().await;
        loop {
            tick.tick().await;

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
                let now = Instant::now();
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

            for id in expired_ids {
                let _ = inner.events.send(PunnuEvent::Invalidate {
                    id,
                    reason: EventReason::TtlExpired,
                });
            }
        }
    });
}
