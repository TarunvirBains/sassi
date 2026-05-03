//! [`PunnuConfig`] — runtime configuration for a [`crate::punnu::Punnu`]
//! instance.
//!
//! Construction is via `PunnuConfig::default()` followed by struct-update
//! syntax, e.g.:
//!
//! ```
//! use sassi::PunnuConfig;
//! let cfg = PunnuConfig {
//!     lru_size: 64,
//!     ..Default::default()
//! };
//! ```
//!
//! All fields are public so consumers can read them back via
//! [`crate::punnu::Punnu::config`]. Defaults are tuned for "typical
//! native consumer" — 10k-entry LRU, 256-event channel, L1-only on
//! backend failure, last-write-wins on conflict, no TTL, no metrics.
//!
//! All fields are load-bearing in the current public surface. Prefer
//! struct-update syntax with `..Default::default()` so future additive
//! config fields do not break callers.

use crate::error::BackendError;
use crate::punnu::events::EventReason;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

/// Tuning knobs for a [`crate::punnu::Punnu`] instance.
///
/// Constructed via the builder ([`crate::punnu::Punnu::builder`]) or
/// passed directly to [`crate::punnu::PunnuBuilder::config`]. Fields
/// are public so consumers can read them back via
/// [`crate::punnu::Punnu::config`] — useful for diagnostics and tests.
///
/// **Forward compatibility:** the canonical construction pattern is
/// `PunnuConfig { lru_size: …, ..Default::default() }`. Future
/// minor releases add fields with sensible defaults; consumers using
/// the `..Default::default()` form upgrade without source changes.
/// Construct *exhaustively* and you'll need to revisit on each minor
/// upgrade.
pub struct PunnuConfig {
    /// LRU capacity in entries. Default `10_000`. Must be non-zero —
    /// the builder enforces this at construction time and panics with a
    /// descriptive message if zero is passed (caller bug, not a runtime
    /// failure mode).
    pub lru_size: usize,

    /// Backing capacity of the broadcast channel that powers the event
    /// stream. Default `256`. Lossy by design: when a subscriber lags
    /// past this many events, the channel drops the oldest events for
    /// that subscriber and surfaces `RecvError::Lagged` on the next
    /// receive. Producer-side `send` calls never block.
    pub event_channel_capacity: usize,

    /// What to do when an attached L2 backend operation fails.
    /// Applies to insert write-through, [`crate::punnu::Punnu::get_async`],
    /// and backend invalidation publishing from
    /// [`crate::punnu::Punnu::invalidate`]. Default
    /// [`BackendFailureMode::L1Only`] — log the error, succeed against
    /// L1 alone.
    pub backend_failure_mode: BackendFailureMode,

    /// What to do when [`crate::punnu::Punnu::insert`] is called for an
    /// id that's already cached. Default
    /// [`OnConflict::LastWriteWins`].
    pub on_conflict: OnConflict,

    /// Default TTL applied to entries inserted via
    /// [`crate::punnu::Punnu::insert`]. `None` (default) means entries
    /// never expire on time. Per-entry overrides via
    /// [`crate::punnu::Punnu::insert_with_ttl`].
    pub default_ttl: Option<Duration>,

    /// Optional background-sweep interval. `None` (default) leaves
    /// expired entries resident in storage; lazy `get` observes them
    /// as absent without cleanup. Later writers remove an expired
    /// entry silently when they touch the same id or when capacity
    /// pressure requires reclaiming expired entries before sampled-LRU
    /// eviction. `Some(d)` spawns a task that scans the L1 every `d`
    /// and removes anything that's already expired.
    /// Spawning requires a target-compatible runtime feature:
    /// `runtime-tokio` on native targets or `runtime-wasm` on wasm32.
    /// Without one, opting into a sweep panics at build time instead
    /// of silently dropping the configured cleanup path.
    pub ttl_sweep_interval: Option<Duration>,

    /// Backend cache-key namespace prepended to all L2 backend keys.
    /// `None` (default) is fine for single-environment deployments;
    /// production setups typically use `"prod_v1"` / `"staging_v1"`,
    /// and tests use a per-run UUID for parallel isolation. L1
    /// storage is unaffected — namespacing governs only L2 keys.
    pub namespace: Option<String>,

    /// Optional observability hook. When `Some`, every event of
    /// interest fires a method on the consumer-supplied
    /// implementation. The trait is intentionally narrow — sassi
    /// commits to surfacing these counters; consumers wire to whatever
    /// metrics layer they already use (Prometheus, OpenTelemetry,
    /// statsd, …) without sassi pulling in a metrics framework.
    /// Default `None` is a no-op that costs nothing at runtime.
    /// Hook is called from L1 hits/misses, writes/invalidations,
    /// `get_or_fetch` slow paths, lazy TTL misses, LRU pressure, and
    /// optional TTL sweep cleanup.
    pub metrics: Option<Arc<dyn PunnuMetrics>>,
}

impl Default for PunnuConfig {
    fn default() -> Self {
        Self {
            lru_size: 10_000,
            event_channel_capacity: 256,
            backend_failure_mode: BackendFailureMode::L1Only,
            on_conflict: OnConflict::LastWriteWins,
            default_ttl: None,
            ttl_sweep_interval: None,
            namespace: None,
            metrics: None,
        }
    }
}

// `PunnuConfig` does not derive `Debug` because the `metrics` field
// (an `Arc<dyn PunnuMetrics>`) is not `Debug`. Manual impl elides the
// metrics handle while keeping the rest debuggable.
impl std::fmt::Debug for PunnuConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PunnuConfig")
            .field("lru_size", &self.lru_size)
            .field("event_channel_capacity", &self.event_channel_capacity)
            .field("backend_failure_mode", &self.backend_failure_mode)
            .field("on_conflict", &self.on_conflict)
            .field("default_ttl", &self.default_ttl)
            .field("ttl_sweep_interval", &self.ttl_sweep_interval)
            .field("namespace", &self.namespace)
            .field("metrics", &self.metrics.as_ref().map(|_| "<configured>"))
            .finish()
    }
}

/// Non-jittered base retry delay for backend attempt `attempt_number`.
///
/// Attempt numbers are 1-based and include the original backend call.
/// Attempt 1 has no preceding delay. Attempt 2 waits 25ms; later
/// attempts double until capped at 1s. Production retry paths add
/// jitter on top of this base to avoid synchronized retry storms.
pub fn retry_delay_for_attempt(attempt_number: u8) -> Duration {
    if attempt_number <= 1 {
        return Duration::ZERO;
    }
    let exponent = u32::from(attempt_number.saturating_sub(2)).min(10);
    Duration::from_millis((25u64.saturating_mul(1u64 << exponent)).min(1_000))
}

/// Behaviour when an L2 backend write-through fails.
///
/// Defaults to [`BackendFailureMode::L1Only`] — the most permissive
/// mode, suitable for caches that are an optimisation rather than a
/// correctness boundary. Consumers with stricter consistency
/// requirements should pick [`BackendFailureMode::Error`] (propagate)
/// or [`BackendFailureMode::Retry`] (retry-with-backoff before falling
/// through).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendFailureMode {
    /// Log the backend error, fall back to L1-only. Insert / get /
    /// invalidate succeed against L1 even if the backend is
    /// unreachable. The recommended default.
    L1Only,

    /// Propagate the backend error to the caller.
    /// `insert` returns `Err(InsertError::BackendFailed(...))`,
    /// `get_async` and `invalidate` return `Err(BackendError)`. Use
    /// when the L2 tier is a correctness requirement, not an
    /// optimisation.
    Error,

    /// Retry the backend operation up to `attempts` total attempts
    /// (including the original call) before falling through to
    /// L1Only behaviour for retryable errors. Backoff is centrally
    /// defined by [`retry_delay_for_attempt`] plus 0-25% jitter in
    /// production call paths. `attempts` must be at least 1.
    Retry {
        /// Number of attempts before giving up.
        attempts: u8,
    },
}

/// Behaviour when [`crate::punnu::Punnu::insert`] is called for an id
/// that's already cached.
///
/// Defaults to [`OnConflict::LastWriteWins`] — straightforward identity-map
/// semantics, suitable for "the most recent value the consumer
/// produced is canonical". `Reject` and `Update` give consumers the
/// other two reasonable shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnConflict {
    /// New insert overwrites the existing entry; emits
    /// [`crate::punnu::PunnuEvent::Insert`].
    LastWriteWins,

    /// New insert returns
    /// [`crate::error::InsertError::Conflict`]; the existing entry is
    /// left in place.
    Reject,

    /// New insert overwrites the existing entry, but emits
    /// [`crate::punnu::PunnuEvent::Update`] (carrying both the old and
    /// the new value) instead of `Insert`.
    Update,
}

/// Cache tier — used by [`PunnuMetrics::record_hit`] to disambiguate
/// L1 (in-memory) hits from L2 (backend) hits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheTier {
    /// L1 — in-process LRU.
    L1,
    /// L2 — pluggable backend (Redis, Postgres, file, memory).
    L2,
}

/// Observability hook. Sassi commits to firing these counters on every
/// event of interest; consumers wire to whatever metrics layer they
/// already use (Prometheus, OpenTelemetry, statsd, …) without sassi
/// pulling in a metrics framework.
///
/// Implementations must be `Send + Sync` because the trait object is
/// shared across the broadcast subscriber side and the operation
/// callsites. All methods take `&self` — implementations typically
/// forward to atomic counters or a metrics-library handle.
///
/// Callbacks run synchronously on the calling operation. Eviction and
/// size callbacks run in Punnu's ordered commit path after the short
/// snapshot write and before the next writer may publish, so
/// implementations must be fast and must not call back into the same
/// [`crate::punnu::Punnu`] instance. Sassi catches and logs callback
/// panics so observability failures do not poison the cache write
/// path.
///
/// `type_name` is `std::any::type_name::<T>()` — pre-baked at compile
/// time, zero-runtime-cost, suitable for a Prometheus label.
pub trait PunnuMetrics: Send + Sync {
    /// A `get` (or `get_or_fetch`) hit served from the named tier.
    fn record_hit(&self, type_name: &'static str, tier: CacheTier);

    /// A `get` miss — neither L1 nor L2 had the entry.
    fn record_miss(&self, type_name: &'static str);

    /// An eviction-class metric observation occurred. For LRU, sweep,
    /// manual, save/delete, and backend invalidation paths this means
    /// an entry left L1. For lazy TTL reads, `TtlExpired` records that
    /// an expired resident entry was treated as absent even though
    /// physical cleanup and `PunnuEvent` emission are deferred to a
    /// writer or sweep. The reason discriminator lets metrics
    /// distinguish capacity pressure, freshness misses, and explicit
    /// invalidation. Takes the full [`EventReason`] taxonomy — metrics
    /// dashboards typically split by every reason, including
    /// system-internal reasons callers cannot directly trigger.
    fn record_eviction(&self, type_name: &'static str, reason: EventReason);

    /// An L2 backend operation failed. Surfaces the underlying
    /// [`BackendError`] so dashboards can split by failure mode
    /// (network vs. serialization vs. not-found).
    fn record_backend_error(&self, type_name: &'static str, err: &BackendError);

    /// End-to-end fetch latency for `get_or_fetch`-style slow paths.
    fn record_fetch_latency(&self, type_name: &'static str, duration: Duration);

    /// L1 entry count, sampled — useful for "is the LRU near its
    /// capacity?" alerts. Sassi calls this opportunistically (after
    /// inserts and invalidations); consumers should treat it as a
    /// gauge sample, not a stream of every change.
    fn record_lru_size(&self, type_name: &'static str, size: usize);
}

pub(crate) fn record_metric_safely(callback: impl FnOnce()) {
    if catch_unwind(AssertUnwindSafe(callback)).is_err() {
        tracing::warn!("PunnuMetrics callback panicked; ignoring metrics sample");
    }
}
