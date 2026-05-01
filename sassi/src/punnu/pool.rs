//! [`Punnu<T>`] — the typed in-memory pool.
//!
//! Holds `Arc<PunnuInner<T>>` so the public `Punnu<T>` is cheap to
//! clone and shareable across tasks; every clone observes the same
//! identity map, the same event stream, and the same configuration.
//!
//! # Concurrency
//!
//! The L1 storage is a [`std::sync::RwLock`] around an `LruCache`
//! rather than an async lock. Two reasons:
//!
//! 1. The spec calls `get` synchronous (§3.5) — `tokio::sync::RwLock`
//!    has no sync `read()`, so picking a sync lock is the only way to
//!    keep the public surface as designed.
//! 2. The `lru` crate's `get` method takes `&mut self` (it touches the
//!    LRU recency order on access), so even read-shaped operations
//!    need exclusive access to the map. A sync `RwLock<LruCache>`
//!    therefore behaves like a sync `Mutex<LruCache>` in practice;
//!    the `RwLock` shape leaves headroom for future LRU implementations
//!    that genuinely support `&self` reads (e.g., a clock-based variant)
//!    without changing the field type.
//!
//! Locks are held only across pure in-memory work (no awaits, no IO),
//! so a sync lock under an async runtime is safe.

use crate::cacheable::Cacheable;
use crate::error::InsertError;
use crate::executor::{DefaultExecutor, PunnuExecutor};
use crate::punnu::config::{OnConflict, PunnuConfig};
use crate::punnu::events::{EventReason, InvalidationReason, PunnuEvent};
use crate::punnu::ttl::Entry;
#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
use crate::punnu::ttl::spawn_sweep;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;
#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
use tokio::sync::Notify;
use tokio::sync::broadcast;

/// Typed in-memory pool — the cache primitive. See module-level docs
/// for concurrency and identity-map contract.
///
/// `Punnu<T>` is a thin handle around an `Arc<PunnuInner<T>>`; cloning
/// a `Punnu<T>` clones the `Arc`, not the underlying state. Multiple
/// clones observe the same identity map, the same event stream, and
/// the same configuration. This is the intended sharing pattern —
/// hand `Punnu<T>` clones to request handlers, background tasks, and
/// scopes alike.
///
/// # Lifecycle
///
/// 1. Construct via [`Punnu::builder`].
/// 2. [`Punnu::insert`] (uses [`PunnuConfig::default_ttl`]) or
///    [`Punnu::insert_with_ttl`] (per-entry TTL override) entries.
/// 3. [`Punnu::get`] entries by id (synchronous; lazy TTL check).
/// 4. [`Punnu::invalidate`] entries explicitly, or let LRU / TTL
///    pressure evict them.
/// 5. Subscribe to [`Punnu::events`] for an observability stream.
///
/// Drop the last `Punnu<T>` clone to release the inner state — the
/// optional background TTL sweep task uses `Arc::downgrade` and
/// exits cleanly when the strong count reaches zero.
pub struct Punnu<T: Cacheable> {
    inner: Arc<PunnuInner<T>>,
}

impl<T: Cacheable> Clone for Punnu<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Internal shared state — held behind `Arc` so `Punnu<T>` is cheaply
/// cloneable.
///
/// `pub(crate)` so the sweep task (Task 6) and the scope handle (Task
/// 11) can observe state directly without going through `Punnu<T>`.
pub(crate) struct PunnuInner<T: Cacheable> {
    /// L1 identity map. See module-level docs for the lock choice
    /// rationale.
    pub(crate) map: RwLock<LruCache<T::Id, Entry<T>>>,

    /// Lossy-by-design event channel; see
    /// [`crate::punnu::PunnuEvent`].
    pub(crate) events: broadcast::Sender<PunnuEvent<T>>,

    /// Configuration captured at build time. Held by reference via
    /// [`Punnu::config`]; not mutated after construction.
    pub(crate) config: PunnuConfig,

    /// Runtime primitives — `spawn`, `sleep`, `now`. Held as
    /// `Arc<dyn PunnuExecutor>` so v0.2's
    /// [`crate::punnu::PunnuConfig::executor`] field plugs in without
    /// any internal refactor; v0.1 always populates this with
    /// `Arc<DefaultExecutor>`. See spec §3.11 / §3.11.1.
    pub(crate) executor: Arc<dyn PunnuExecutor>,

    /// Readiness signal fired on the sweep task's first poll, *before*
    /// the first sleep. Tests `await` this before
    /// `tokio::time::advance(...)` to guarantee the sleep is
    /// registered against the test's virtual clock. `None` when no
    /// sweep is configured (the field is still allocated to keep the
    /// struct shape stable; `pub(crate)` so the test-helper accessor
    /// on `Punnu<T>` can reach it). Closes
    /// <https://github.com/TarunvirBains/sassi/issues/4>.
    #[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
    pub(crate) sweep_initialised: Option<Arc<Notify>>,
}

impl<T: Cacheable> Punnu<T> {
    /// Begin building a [`Punnu<T>`]. See [`PunnuBuilder`].
    ///
    /// # Example
    ///
    /// ```
    /// use sassi::Punnu;
    ///
    /// # struct User { id: i64 }
    /// # impl sassi::Cacheable for User {
    /// #     type Id = i64;
    /// #     type Fields = UserFields;
    /// #     fn id(&self) -> i64 { self.id }
    /// #     fn fields() -> UserFields { UserFields }
    /// # }
    /// # #[derive(Default)] struct UserFields;
    /// let pool: Punnu<User> = Punnu::builder().build();
    /// ```
    pub fn builder() -> PunnuBuilder<T> {
        PunnuBuilder::new()
    }

    /// Insert a value into the pool.
    ///
    /// Identity-map semantics: the entry is keyed by `value.id()`. If
    /// an entry with the same id is already cached, behaviour follows
    /// [`PunnuConfig::on_conflict`]:
    ///
    /// - [`OnConflict::LastWriteWins`] (default) — the new value
    ///   replaces the existing one; emits
    ///   [`PunnuEvent::Insert`].
    /// - [`OnConflict::Reject`] — returns
    ///   [`InsertError::Conflict`]; the existing entry is left in
    ///   place.
    /// - [`OnConflict::Update`] — the new value replaces the existing
    ///   one; emits [`PunnuEvent::Update`] carrying both the old and
    ///   the new value.
    ///
    /// If the insert pushes the LRU past `lru_size`, the
    /// least-recently-used entry is evicted and an
    /// [`EventReason::LruEvict`] event fires before the
    /// `Insert` / `Update` event.
    ///
    /// `async` because L2 backend write-through (a later task) is
    /// async; the L1-only path resolves immediately.
    ///
    /// # Errors
    ///
    /// - [`InsertError::Conflict`] if [`OnConflict::Reject`] is
    ///   configured and the id already exists.
    /// - [`InsertError::BackendFailed`] / [`InsertError::Serialization`]
    ///   become reachable when L2 backends land in a later task.
    ///
    /// # TTL
    ///
    /// Uses [`PunnuConfig::default_ttl`] when set, otherwise the entry
    /// has no expiry deadline. Per-entry overrides go through
    /// [`Punnu::insert_with_ttl`].
    pub async fn insert(&self, value: T) -> Result<Arc<T>, InsertError> {
        let ttl = self.inner.config.default_ttl;
        let arc = Arc::new(value);
        self.insert_arc_internal(arc, ttl).await
    }

    /// Insert with an explicit TTL — overrides
    /// [`PunnuConfig::default_ttl`] for this entry. Pass any large
    /// duration (e.g., `Duration::MAX`) to effectively disable TTL
    /// for this entry without touching the pool's default.
    ///
    /// All other semantics match [`Punnu::insert`] — identity-map,
    /// `OnConflict` policy, LRU pressure, event emission. The `ttl`
    /// is added to `Instant::now()` at the moment of insert; clock
    /// adjustments after insert do not change the deadline.
    pub async fn insert_with_ttl(&self, value: T, ttl: Duration) -> Result<Arc<T>, InsertError> {
        let arc = Arc::new(value);
        self.insert_arc_internal(arc, Some(ttl)).await
    }

    /// Internal insert — shared by `insert` and `insert_with_ttl`.
    /// `ttl` is the absolute TTL applied to this entry (`None` means
    /// no expiry).
    async fn insert_arc_internal(
        &self,
        arc: Arc<T>,
        ttl: Option<Duration>,
    ) -> Result<Arc<T>, InsertError> {
        let id = arc.id();
        let expires_at = ttl.map(|d| self.inner.executor.now() + d);
        let entry = Entry::with_expiry(arc.clone(), expires_at);

        // Locking + push are pure in-memory work — no awaits while
        // holding the lock.
        let outcome = {
            let mut map = self.inner.map.write().expect(
                "Punnu L1 lock poisoned — a previous panic left it in an inconsistent state",
            );

            // Probe whether the id is already present *before* we
            // mutate, so we can decide on `Reject` / `Update` without
            // having to undo a `push`. `LruCache::peek` does not
            // touch recency order, which is what we want here.
            let existing = map.peek(&id).cloned();

            if existing.is_some() && matches!(self.inner.config.on_conflict, OnConflict::Reject) {
                return Err(InsertError::Conflict);
            }

            // `LruCache::push` returns Some((k, v)) for both
            // replacement (k == inserted id) and capacity-driven
            // eviction (k != inserted id). We disambiguate by
            // comparing keys.
            let pushed = map.push(id.clone(), entry);
            InsertOutcome {
                existing,
                replaced_or_evicted: pushed,
            }
        };

        // Drop the lock before emitting events — broadcast `send`
        // does no IO but does signal subscribers, and we don't want
        // any subscriber-side latency to add to lock-hold time.

        // First emit the LRU-eviction event, if any. Replace and
        // capacity-eviction look identical at the `lru` API level;
        // disambiguate by key comparison.
        if let Some((evicted_id, _evicted_entry)) = outcome.replaced_or_evicted
            && evicted_id != id
        {
            let _ = self.inner.events.send(PunnuEvent::Invalidate {
                id: evicted_id,
                reason: EventReason::LruEvict,
            });
        }

        // Then emit the insert / update event, choosing the variant
        // by `OnConflict` policy.
        let event = match (outcome.existing, self.inner.config.on_conflict) {
            (Some(old), OnConflict::Update) => PunnuEvent::Update {
                old: old.value,
                new: arc.clone(),
            },
            // LastWriteWins (or any other path that didn't bail out
            // earlier) emits `Insert`. The "Reject" path returned
            // before reaching this point.
            _ => PunnuEvent::Insert { value: arc.clone() },
        };
        let _ = self.inner.events.send(event);

        Ok(arc)
    }

    /// Synchronous L1 lookup. Returns `Some(Arc<T>)` if the id is
    /// cached and unexpired; `None` if it isn't cached **or** the
    /// entry's TTL has elapsed.
    ///
    /// On hit, refreshes the entry's LRU recency. (`LruCache::get`
    /// takes `&mut self` precisely because it updates the recency
    /// list — that's why this method takes the write lock even
    /// though it's read-shaped at the API level. See module-level
    /// docs.)
    ///
    /// # TTL semantics (lazy expiry path)
    ///
    /// If the entry exists but `expires_at <= Instant::now()`:
    ///
    /// 1. The entry is removed from L1.
    /// 2. A [`PunnuEvent::Invalidate`] with reason
    ///    [`EventReason::TtlExpired`] is emitted.
    /// 3. `None` is returned.
    ///
    /// The expiry check + removal happen under the same write lock
    /// so concurrent `get`s observe a consistent state — at most one
    /// `TtlExpired` event fires for a given expired entry, even
    /// under contention.
    pub fn get(&self, id: &T::Id) -> Option<Arc<T>> {
        // Fast path: peek first to avoid the write lock when the id
        // is missing entirely. `peek` does not touch recency, so the
        // lookup-on-hit path still needs the write lock — but a miss
        // is the common cold-cache case, so the read-only peek is
        // worth the branch.
        let expired = {
            // Sample `now` once before taking the write lock so the
            // decision is consistent across the peek + pop without
            // re-reading the clock under the lock. `now` is a cheap
            // monotonic read regardless of executor.
            let now = self.inner.executor.now();
            let mut map = self.inner.map.write().expect(
                "Punnu L1 lock poisoned — a previous panic left it in an inconsistent state",
            );
            // Peek first to make the expiry decision *without*
            // touching recency. If the peeked entry is fresh, fall
            // through to `get` (which touches recency) and return
            // the value. If it's expired, pop it under the same
            // lock — that's the race-safe spot to decide who fires
            // `TtlExpired`.
            let peeked = map.peek(id)?;
            if peeked.is_expired_at(now) {
                map.pop(id);
                true
            } else {
                // `get` is guaranteed to find the entry — we just
                // peeked it under the same write lock.
                let entry = map
                    .get(id)
                    .expect("entry present (just peeked under same lock)");
                return Some(entry.value.clone());
            }
        };

        if expired {
            let _ = self.inner.events.send(PunnuEvent::Invalidate {
                id: id.clone(),
                reason: EventReason::TtlExpired,
            });
        }
        None
    }

    /// Drop a single entry by id. No-op if the id is not cached.
    /// Emits [`PunnuEvent::Invalidate`] with the supplied reason
    /// (lifted into the wider [`EventReason`] taxonomy) when an entry
    /// was actually removed.
    ///
    /// Accepts only the [`InvalidationReason`] subset
    /// (`Manual` / `OnSave` / `OnDelete`) — system-internal reasons
    /// like LRU eviction or TTL expiry are constructed by sassi itself
    /// and surface on the event stream as [`EventReason::LruEvict`] /
    /// [`EventReason::TtlExpired`], not via this entry point.
    ///
    /// `async` because L2 backend invalidation (a later task) is
    /// async; the L1-only path resolves immediately.
    pub async fn invalidate(&self, id: &T::Id, reason: InvalidationReason) {
        self.invalidate_internal(id, EventReason::from(reason))
            .await
    }

    /// Internal invalidation entry point — accepts the full
    /// [`EventReason`] taxonomy so sassi-internal call sites (LRU
    /// pressure that bypasses `LruCache::push`, TTL sweep, future
    /// backend-driven invalidation) can emit system-internal reasons
    /// without going through [`InvalidationReason`].
    ///
    /// Identical L1 semantics to [`Punnu::invalidate`] otherwise.
    /// `pub(crate)` so it cannot be used by external callers — that
    /// would defeat the public-vs-internal split that motivates the
    /// two enums.
    pub(crate) async fn invalidate_internal(&self, id: &T::Id, reason: EventReason) {
        let removed = {
            let mut map = self.inner.map.write().expect(
                "Punnu L1 lock poisoned — a previous panic left it in an inconsistent state",
            );
            map.pop(id).is_some()
        };
        if removed {
            let _ = self.inner.events.send(PunnuEvent::Invalidate {
                id: id.clone(),
                reason,
            });
        }
    }

    /// Number of entries currently in the L1.
    ///
    /// Snapshots the current size; concurrent inserts / invalidates
    /// against another `Punnu<T>` clone may change the value before
    /// the caller reads it. Suitable for diagnostics and tests, not
    /// for "is the entry I just inserted definitely visible?" checks
    /// — use `get` for that.
    pub fn len(&self) -> usize {
        self.inner
            .map
            .read()
            .expect("Punnu L1 lock poisoned — a previous panic left it in an inconsistent state")
            .len()
    }

    /// `true` iff [`Punnu::len`] is zero. Convenience predicate; same
    /// snapshot semantics as `len`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Subscribe to the broadcast event stream.
    ///
    /// Each subscriber gets a fresh `Receiver` that observes events
    /// from this point forward — backfill of past events is not
    /// supported. Lossy under load: if the subscriber lags past
    /// [`PunnuConfig::event_channel_capacity`], the channel drops the
    /// oldest events and the next `recv` returns
    /// `RecvError::Lagged(skipped)`. The producer never blocks.
    pub fn events(&self) -> broadcast::Receiver<PunnuEvent<T>> {
        self.inner.events.subscribe()
    }

    /// Borrow the captured configuration.
    pub fn config(&self) -> &PunnuConfig {
        &self.inner.config
    }

    // `scope()` (the predicate-driven query handle) lands in Task 11
    // alongside the `MemQ<T>` extension algebra. Until then, callers
    // compose with `Punnu::get` / iteration over `events()`. See
    // `docs/superpowers/plans/2026-05-01-sassi-v0.1.0.md` Task 11.

    /// Test-only readiness handshake — resolves once the background
    /// TTL sweep task has been polled the first time and is parked on
    /// its initial `executor.sleep(interval)`. Tests `await` this
    /// before calling `tokio::time::advance(...)` so the sleep is
    /// guaranteed to be registered against the test's virtual clock.
    ///
    /// Returns `None` when no sweep was configured
    /// (`PunnuConfig::ttl_sweep_interval == None`) — the readiness
    /// signal only exists when there's a sweep to be ready about.
    ///
    /// `#[doc(hidden)]` because this is a test-helper escape hatch,
    /// not part of the v0.1 public surface. Tests in this crate
    /// import it; downstream code shouldn't.
    /// Closes <https://github.com/TarunvirBains/sassi/issues/4>.
    #[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
    #[doc(hidden)]
    pub fn _test_sweep_initialised(&self) -> Option<Arc<Notify>> {
        self.inner.sweep_initialised.clone()
    }
}

/// Outcome of an `LruCache::push` pass — captured under the lock and
/// returned for event emission once the lock is released.
struct InsertOutcome<T: Cacheable> {
    /// Snapshot of the prior entry, if any. Held so the `Update`
    /// event can carry `old`.
    existing: Option<Entry<T>>,
    /// Whatever `LruCache::push` returned — `Some` for both replace
    /// and capacity-driven eviction; disambiguate by key comparison
    /// against the inserted id.
    replaced_or_evicted: Option<(T::Id, Entry<T>)>,
}

/// Builder for [`Punnu<T>`]. Construct via [`Punnu::builder`].
///
/// The builder pattern lets the v0.1 surface stay narrow while
/// reserving room for `.with_backend(...)`, `.with_executor(...)`,
/// `.with_tenant(...)` setters that land in later tasks. Today the
/// only active configuration path is `.config(c)`, which captures a
/// fully-formed [`PunnuConfig`].
pub struct PunnuBuilder<T: Cacheable> {
    config: PunnuConfig,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: Cacheable> PunnuBuilder<T> {
    /// Fresh builder with default configuration. Most consumers
    /// reach this via [`Punnu::builder`].
    pub fn new() -> Self {
        Self {
            config: PunnuConfig::default(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Replace the captured configuration wholesale. Use struct-update
    /// syntax to override individual fields:
    ///
    /// ```
    /// # use sassi::{Punnu, PunnuConfig};
    /// # struct User { id: i64 }
    /// # impl sassi::Cacheable for User {
    /// #     type Id = i64;
    /// #     type Fields = UserFields;
    /// #     fn id(&self) -> i64 { self.id }
    /// #     fn fields() -> UserFields { UserFields }
    /// # }
    /// # #[derive(Default)] struct UserFields;
    /// let pool: Punnu<User> = Punnu::builder()
    ///     .config(PunnuConfig { lru_size: 128, ..Default::default() })
    ///     .build();
    /// ```
    pub fn config(mut self, config: PunnuConfig) -> Self {
        self.config = config;
        self
    }

    /// Finalize the builder.
    ///
    /// If [`PunnuConfig::ttl_sweep_interval`] is `Some`, spawns a
    /// background tokio task that scans the L1 every interval tick
    /// for TTL-expired entries (gated behind the `runtime-tokio`
    /// feature; WASM consumers wait on the executor abstraction
    /// landing in a later task). The sweep task uses
    /// [`std::sync::Weak`] to detect drop of every `Punnu<T>` clone
    /// — when the strong count of `PunnuInner<T>` falls to zero,
    /// the upgrade fails, and the loop exits cleanly. No explicit
    /// shutdown handle.
    ///
    /// # Panics
    ///
    /// Panics if any of the following config invariants is violated;
    /// the builder catches the bad shape at construction time so the
    /// failure surfaces with a clear message instead of a cryptic panic
    /// from a downstream primitive (`tokio::time::interval(0)` or
    /// `broadcast::channel(0)`):
    ///
    /// - `config.lru_size == 0` — a zero-capacity LRU evicts every
    ///   insert immediately, which is a programmer error.
    /// - `config.ttl_sweep_interval == Some(Duration::ZERO)` — would
    ///   reach `tokio::time::interval(0)` and panic at runtime. Use
    ///   `None` to disable the sweep.
    /// - `config.event_channel_capacity == 0` — would reach
    ///   `broadcast::channel(0)` and panic at runtime. The minimum
    ///   sensible value is `1`.
    pub fn build(self) -> Punnu<T> {
        let cap = NonZeroUsize::new(self.config.lru_size).unwrap_or_else(|| {
            panic!(
                "PunnuConfig::lru_size must be non-zero (got 0); \
                 a zero-capacity LRU evicts every insert immediately"
            )
        });
        if let Some(d) = self.config.ttl_sweep_interval {
            assert!(
                !d.is_zero(),
                "PunnuConfig::ttl_sweep_interval must be greater than Duration::ZERO; \
                 use None to disable the background sweep"
            );
        }
        assert!(
            self.config.event_channel_capacity > 0,
            "PunnuConfig::event_channel_capacity must be greater than 0; \
             the broadcast channel rejects a zero capacity"
        );
        let sweep_interval = self.config.ttl_sweep_interval;
        let (events, _) = broadcast::channel(self.config.event_channel_capacity);
        let executor: Arc<dyn PunnuExecutor> = Arc::new(DefaultExecutor);

        #[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
        let sweep_initialised = sweep_interval.map(|_| Arc::new(Notify::new()));

        let inner = Arc::new(PunnuInner {
            map: RwLock::new(LruCache::new(cap)),
            events,
            config: self.config,
            executor,
            #[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
            sweep_initialised: sweep_initialised.clone(),
        });

        // Spawn the sweep before constructing the public handle so
        // the weak ref is captured against the same `Arc` we hand
        // back. With `runtime-tokio` off, the call site is a no-op.
        #[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
        spawn_sweep_if_configured(&inner, sweep_interval, sweep_initialised);
        #[cfg(not(any(feature = "runtime-tokio", feature = "runtime-wasm")))]
        {
            let _ = sweep_interval; // avoid unused warning
        }

        Punnu { inner }
    }
}

/// Spawn the TTL-sweep task when both the feature is enabled and the
/// config requested a sweep interval. Pulled out of `build()` so the
/// `cfg` gate is a single statement instead of branching the body.
///
/// `sweep_initialised` is the readiness handshake the sweep fires on
/// first poll; tests await it before advancing virtual time. `Some`
/// iff `interval` is `Some` (1:1 invariant established at the call
/// site in [`PunnuBuilder::build`]).
#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
fn spawn_sweep_if_configured<T: Cacheable>(
    inner: &Arc<PunnuInner<T>>,
    interval: Option<Duration>,
    sweep_initialised: Option<Arc<Notify>>,
) {
    if let (Some(interval), Some(notify)) = (interval, sweep_initialised) {
        spawn_sweep(Arc::downgrade(inner), interval, notify);
    }
}

impl<T: Cacheable> Default for PunnuBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}
