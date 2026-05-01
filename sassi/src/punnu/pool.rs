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
use crate::error::{FetchError, InsertError};
use crate::executor::{DefaultExecutor, PunnuExecutor};
use crate::predicate::MemQ;
use crate::punnu::config::{CacheTier, OnConflict, PunnuConfig};
use crate::punnu::events::{EventReason, InvalidationReason, PunnuEvent};
use crate::punnu::scope::PunnuScope;
use crate::punnu::single_flight::InFlightRegistry;
use crate::punnu::ttl::Entry;
#[cfg(any(feature = "runtime-tokio", feature = "runtime-wasm"))]
use crate::punnu::ttl::spawn_sweep;
use lru::LruCache;
use std::future::Future;
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
/// `pub(crate)` so the sweep task and the (future) scope handle can
/// observe state directly without going through `Punnu<T>`.
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

    /// Single-flight in-flight fetch registry — deduplicates
    /// concurrent [`Punnu::get_or_fetch`] callers for the same id so
    /// the consumer's fetcher closure runs exactly once per cold
    /// fetch. See [`crate::punnu::single_flight`] for the cancellation
    /// contract.
    pub(crate) in_flight: InFlightRegistry<T>,
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
        let (outcome, post_len) = {
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
            let post_len = map.len();
            (
                InsertOutcome {
                    existing,
                    replaced_or_evicted: pushed,
                },
                post_len,
            )
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
            self.metrics_record_eviction(EventReason::LruEvict);
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

        // Sample the LRU size after every insert. Spec §3.5.1 calls
        // this an opportunistic gauge — emit on a meaningful change
        // (post-insert, post-invalidate) rather than streaming on
        // every read.
        self.metrics_record_lru_size(post_len);

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
        // Three terminal outcomes — the locked block decides which:
        // - hit: return Some(arc), record_hit
        // - miss (id absent entirely): return None, record_miss
        // - expired: pop, emit TtlExpired event, record_miss + record_eviction
        enum GetOutcome<T> {
            Hit(Arc<T>),
            Miss,
            Expired,
        }

        let outcome = {
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
            match map.peek(id) {
                None => GetOutcome::Miss,
                Some(peeked) => {
                    if peeked.is_expired_at(now) {
                        map.pop(id);
                        GetOutcome::Expired
                    } else {
                        // `get` is guaranteed to find the entry — we
                        // just peeked it under the same write lock.
                        let entry = map
                            .get(id)
                            .expect("entry present (just peeked under same lock)");
                        GetOutcome::Hit(entry.value.clone())
                    }
                }
            }
        };

        match outcome {
            GetOutcome::Hit(arc) => {
                self.metrics_record_hit(CacheTier::L1);
                Some(arc)
            }
            GetOutcome::Miss => {
                self.metrics_record_miss();
                None
            }
            GetOutcome::Expired => {
                let _ = self.inner.events.send(PunnuEvent::Invalidate {
                    id: id.clone(),
                    reason: EventReason::TtlExpired,
                });
                // TTL-driven expiry counts as both an eviction (entry
                // left the cache) and a miss (the get returned None).
                // Dashboards typically split eviction by reason, so we
                // emit both — eviction with TtlExpired and the miss
                // counter. Aligns with spec §3.5.1's "metrics
                // dashboards typically split by every reason".
                self.metrics_record_eviction(EventReason::TtlExpired);
                self.metrics_record_miss();
                None
            }
        }
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
        let (removed, post_len) = {
            let mut map = self.inner.map.write().expect(
                "Punnu L1 lock poisoned — a previous panic left it in an inconsistent state",
            );
            let removed = map.pop(id).is_some();
            (removed, map.len())
        };
        if removed {
            let _ = self.inner.events.send(PunnuEvent::Invalidate {
                id: id.clone(),
                reason,
            });
            self.metrics_record_eviction(reason);
            self.metrics_record_lru_size(post_len);
        }
    }

    /// Get-or-fetch convenience for the lazy-fetch-on-miss pattern.
    ///
    /// On L1 hit, returns the cached `Arc<T>` immediately (no fetcher
    /// invocation). On miss, calls `fetcher`; if it returns
    /// `Some(value)`, inserts the value into L1 (so a subsequent
    /// `get` is a hit) and returns `Some(arc)`. If the fetcher
    /// returns `None`, the cache is left untouched and `None` is
    /// returned — distinct from "fetch failed".
    ///
    /// # Single-flight coalescing (spec §3.5.1)
    ///
    /// Concurrent `get_or_fetch` calls for the same `id` deduplicate:
    /// exactly one fetcher runs per cold fetch, even when N callers
    /// race. Subsequent in-flight callers `await` the same
    /// [`futures::future::Shared`] future. On completion the registry
    /// slot is cleared and the result lands in L1.
    ///
    /// # Cancellation contract
    ///
    /// Four owner-loss cases, all deterministic:
    ///
    /// 1. **Originating caller drops, peers polling.** The fetch
    ///    stays alive while ≥1 peer awaits.
    /// 2. **All awaiters drop simultaneously.** Fetch is cancelled;
    ///    registry slot is cleared. Subsequent calls retry from cold.
    /// 3. **Fetcher panics.** Every awaiter receives
    ///    [`crate::error::FetchError::FetcherPanic`]. The registry
    ///    slot is cleared.
    /// 4. **Caller-imposed deadline.** Punnu does not impose one.
    ///    Wrap the call in `tokio::time::timeout(...)` for case 4 —
    ///    that surfaces as case 1 from the registry's perspective.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use sassi::Punnu;
    /// # struct User { id: i64 }
    /// # impl sassi::Cacheable for User {
    /// #     type Id = i64;
    /// #     type Fields = UserFields;
    /// #     fn id(&self) -> i64 { self.id }
    /// #     fn fields() -> UserFields { UserFields }
    /// # }
    /// # #[derive(Default)] struct UserFields;
    /// # async fn run(p: Punnu<User>) -> Result<(), sassi::FetchError> {
    /// let user = p.get_or_fetch(&42, |id| async move {
    ///     // Pretend this is a database call.
    ///     Ok::<_, sassi::FetchError>(Some(User { id }))
    /// }).await?;
    /// # Ok(()) }
    /// ```
    pub async fn get_or_fetch<F, Fut>(
        &self,
        id: &T::Id,
        fetcher: F,
    ) -> Result<Option<Arc<T>>, FetchError>
    where
        F: FnOnce(T::Id) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Option<T>, FetchError>> + Send + 'static,
    {
        // L1 fast path — also runs the lazy TTL check via `get`.
        if let Some(arc) = self.get(id) {
            return Ok(Some(arc));
        }

        // Time the fetch path end-to-end (registry attach + fetcher
        // run + L1 insert) so the latency includes coalescing
        // overhead. Spec §3.5.1: `record_fetch_latency` fires for
        // every `get_or_fetch` invocation that hits the slow path
        // (L1 miss). Hits don't pay fetch latency, so they don't
        // emit the histogram point.
        let start = self.inner.executor.now();

        // Single-flight coalescing on miss. The L1 insert runs
        // **inside** the shared future body via the `on_fetched`
        // callback — exactly once per fetch, regardless of how many
        // peers attached. Without this, every coalesced peer would
        // re-run `insert_arc_into_l1` after the await, multiplying
        // events / TTL deadlines / OnConflict evaluations.
        //
        // `on_fetched` returns the canonical `Arc<T>`: the freshly-
        // fetched one on the success path, or the already-cached
        // value when a non-expired entry beat us to the slot during
        // the fetch. The atomic `insert_arc_or_existing` handles
        // both the "fresh insert" and "existing entry, return it"
        // cases under one L1 write lock — no expired-conflict race.
        // `OnConflict` policy does not apply on this path:
        // `get_or_fetch`'s contract is "ensure the cache has
        // SOMETHING at this id, return it," not "follow the
        // configured insert policy on a fresh fetch."
        let inner_weak = Arc::downgrade(&self.inner);
        let on_fetched = move |_id: T::Id, arc: Arc<T>| {
            let inner_weak = inner_weak.clone();
            async move {
                // If the Punnu was dropped during the fetch, just
                // return the freshly-fetched Arc — it'll never reach
                // L1, but the originating caller still sees their
                // value.
                let Some(inner) = inner_weak.upgrade() else {
                    return arc;
                };
                Punnu { inner }.insert_arc_or_existing(arc).await
            }
        };
        let result = self
            .inner
            .in_flight
            .get_or_fetch(id, fetcher, on_fetched)
            .await?;

        let elapsed = self.inner.executor.now() - start;
        self.metrics_record_fetch_latency(elapsed);

        Ok(result)
    }

    /// Batch get-or-fetch. Splits `ids` into "already cached"
    /// (returned immediately) and "missing" (passed to
    /// `batch_fetcher` as a single call). Avoids N round-trips when
    /// the consumer naturally has a list of ids to resolve.
    ///
    /// # Single-flight semantics (v0.1)
    ///
    /// Within a single batch call, missing ids are deduplicated
    /// before the batch fetcher is invoked (input may contain dupes).
    /// Across concurrent batch calls, batch-level deduplication is
    /// **not** implemented in v0.1 — two concurrent
    /// `get_or_fetch_many(&[1, 2, 3])` calls invoke the batch fetcher
    /// twice for the same set. Per-id single-flight coalescing across
    /// concurrent *individual* `get_or_fetch` calls still applies as
    /// usual; only the batch path skips the cross-batch dedup.
    ///
    /// Tracked as a v0.2 enhancement.
    ///
    /// # Result ordering
    ///
    /// The returned `Vec<Arc<T>>` does **not** preserve the input
    /// `ids` order. Hits come first (in the order they were found
    /// in L1), then fetched entries (in the order the batch fetcher
    /// returned them). Consumers needing positional lookup should
    /// build a `HashMap<T::Id, Arc<T>>` from the result.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use sassi::Punnu;
    /// # struct User { id: i64 }
    /// # impl sassi::Cacheable for User {
    /// #     type Id = i64;
    /// #     type Fields = UserFields;
    /// #     fn id(&self) -> i64 { self.id }
    /// #     fn fields() -> UserFields { UserFields }
    /// # }
    /// # #[derive(Default)] struct UserFields;
    /// # async fn run(p: Punnu<User>) -> Result<(), sassi::FetchError> {
    /// let users = p.get_or_fetch_many(&[1, 2, 3, 4, 5], |missing| async move {
    ///     // Pretend this is one batched DB call.
    ///     Ok::<_, sassi::FetchError>(missing.into_iter().map(|id| User { id }).collect())
    /// }).await?;
    /// # let _ = users; Ok(()) }
    /// ```
    pub async fn get_or_fetch_many<F, Fut>(
        &self,
        ids: &[T::Id],
        batch_fetcher: F,
    ) -> Result<Vec<Arc<T>>, FetchError>
    where
        F: FnOnce(Vec<T::Id>) -> Fut + Send,
        Fut: Future<Output = Result<Vec<T>, FetchError>> + Send,
    {
        // Split cached vs missing. We dedup the `missing` list so the
        // batch fetcher isn't asked for the same id twice within one
        // call (consumers may pass dupes; batch backends rarely
        // dedupe themselves).
        let mut hits = Vec::new();
        let mut missing: Vec<T::Id> = Vec::new();
        let mut seen_missing: std::collections::HashSet<T::Id> = std::collections::HashSet::new();
        for id in ids {
            if let Some(arc) = self.get(id) {
                hits.push(arc);
            } else if seen_missing.insert(id.clone()) {
                missing.push(id.clone());
            }
        }

        if missing.is_empty() {
            return Ok(hits);
        }

        // One batch fetch covers every missing id. Cross-batch
        // dedup is documented as a v0.2 enhancement above.
        let fetched = batch_fetcher(missing).await?;
        let mut fetched_arcs = Vec::with_capacity(fetched.len());
        for value in fetched {
            // Route through the public `insert` path so OnConflict,
            // event emission, and (later) metrics fire as if the
            // consumer had inserted manually.
            let arc = self.insert(value).await?;
            fetched_arcs.push(arc);
        }
        hits.extend(fetched_arcs);
        Ok(hits)
    }

    /// Insert a pre-built `Arc<T>` into L1 without cloning the
    /// inner value. Used by [`Punnu::get_or_fetch`] after a
    /// single-flight fetch — the fetcher returns `T`, single-flight
    /// wraps it in an `Arc<T>` for cheap multi-awaiter sharing, and
    /// this method threads the arc into L1 with the same OnConflict
    /// + event-emission semantics as [`Punnu::insert`].
    ///
    /// `pub(crate)` because consumers should use `insert` (which
    /// handles boxing); this is the internal escape hatch for the
    /// single-flight path.
    /// Single-flight insert variant: insert `arc` into L1 if the slot
    /// is empty *or holds an expired entry*; otherwise return the
    /// already-cached non-expired `Arc<T>`. Atomic under one L1 write
    /// lock — no expired-conflict race window.
    ///
    /// Used by [`Punnu::get_or_fetch`]'s `on_fetched` callback.
    /// Resolves the BLOCK-M2 corner case where the consumer's
    /// `OnConflict::Reject` policy collides with an entry that is
    /// expired but not yet lazily popped: the standard `insert` path
    /// would reject; this method treats expired entries as "absent"
    /// and inserts the freshly-fetched value, satisfying
    /// `get_or_fetch`'s documented post-condition that a subsequent
    /// `get` hits.
    ///
    /// Returns the canonical `Arc<T>` — the freshly-inserted one if
    /// the slot was empty/expired, the existing one if a non-expired
    /// entry was present.
    ///
    /// `OnConflict` policy does not apply here. The behaviour is
    /// always "insert if absent or expired; else return existing" —
    /// `get_or_fetch`'s contract is "ensure the cache has SOMETHING
    /// at this id, return it," not "follow the configured insert
    /// policy."
    pub(crate) async fn insert_arc_or_existing(&self, arc: Arc<T>) -> Arc<T> {
        let id = arc.id();
        let ttl = self.inner.config.default_ttl;
        let now = self.inner.executor.now();
        let expires_at = ttl.map(|d| now + d);
        let entry = Entry::with_expiry(arc.clone(), expires_at);

        // Decision happens under a single write lock so the "is
        // there a non-expired entry?" check + insert-if-not race-
        // free. Return value is the canonical `Arc<T>` — the existing
        // one if non-expired, the inserted one otherwise.
        enum InsertOrExistingOutcome<T: Cacheable> {
            ReturnedExisting(Arc<T>),
            Inserted {
                lru_evicted: Option<T::Id>,
                post_len: usize,
            },
        }

        let outcome = {
            let mut map = self.inner.map.write().expect(
                "Punnu L1 lock poisoned — a previous panic left it in an inconsistent state",
            );
            match map.peek(&id) {
                Some(existing) if !existing.is_expired_at(now) => {
                    InsertOrExistingOutcome::ReturnedExisting(existing.value.clone())
                }
                _ => {
                    // Slot empty OR existing entry is expired. Push the
                    // fresh entry — `LruCache::push` replaces the
                    // expired entry in place (or evicts a different
                    // entry if at capacity).
                    let pushed = map.push(id.clone(), entry);
                    let post_len = map.len();
                    let lru_evicted =
                        pushed.and_then(|(k, _v)| if k != id { Some(k) } else { None });
                    InsertOrExistingOutcome::Inserted {
                        lru_evicted,
                        post_len,
                    }
                }
            }
        };

        match outcome {
            InsertOrExistingOutcome::ReturnedExisting(existing) => existing,
            InsertOrExistingOutcome::Inserted {
                lru_evicted,
                post_len,
            } => {
                // Emit events outside the lock.
                if let Some(evicted_id) = lru_evicted {
                    let _ = self.inner.events.send(PunnuEvent::Invalidate {
                        id: evicted_id,
                        reason: EventReason::LruEvict,
                    });
                    self.metrics_record_eviction(EventReason::LruEvict);
                }
                let _ = self
                    .inner
                    .events
                    .send(PunnuEvent::Insert { value: arc.clone() });
                self.metrics_record_lru_size(post_len);
                arc
            }
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

    /// Build an owned query scope over this pool.
    ///
    /// The scope captures a cloned `Punnu<T>` handle, not a borrow, so
    /// callers can move the query handle freely. The supplied
    /// operations remain lazy until a terminal method such as
    /// [`PunnuScope::collect`] or [`PunnuScope::iter`] runs.
    pub fn scope(&self, ops: impl Into<Vec<MemQ<T>>>) -> PunnuScope<T> {
        PunnuScope::new(Arc::new(self.clone()), ops)
    }

    /// Snapshot unexpired entries for an owned query scope.
    ///
    /// Scope collection is read-shaped: it skips expired entries but
    /// does not perform lazy TTL removal or emit invalidation events.
    /// The public `get` path remains the place where lazy expiry
    /// mutates the L1.
    pub(crate) fn snapshot_unexpired(&self) -> Vec<Arc<T>> {
        let now = self.inner.executor.now();
        let map =
            self.inner.map.read().expect(
                "Punnu L1 lock poisoned — a previous panic left it in an inconsistent state",
            );
        map.iter()
            .filter(|(_, entry)| !entry.is_expired_at(now))
            .map(|(_, entry)| entry.value.clone())
            .collect()
    }

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

    // ---------------------------------------------------------------
    // Metrics helpers — internal `record_*` shims that no-op when
    // `PunnuConfig::metrics` is `None`. Wired from the call sites
    // above (`get`, `insert_arc_internal`, `invalidate_internal`,
    // `get_or_fetch`). The shim pattern:
    //   - returns `()` so call sites can `.metrics_record_*()` on
    //     `&self` with no error handling.
    //   - reads `type_name` once via `std::any::type_name::<T>()`
    //     (zero runtime cost; resolved at compile time per generic
    //     instantiation). Spec §3.5.1 documents that `type_name` is
    //     a metrics label, not a stable cross-version protocol id —
    //     wire-format keys use a different identifier.
    //   - the `if let Some(m) = ...` guard ensures the no-op path
    //     compiles to a single null-check at the call site.
    // ---------------------------------------------------------------

    fn metrics_record_hit(&self, tier: CacheTier) {
        if let Some(m) = &self.inner.config.metrics {
            m.record_hit(std::any::type_name::<T>(), tier);
        }
    }

    fn metrics_record_miss(&self) {
        if let Some(m) = &self.inner.config.metrics {
            m.record_miss(std::any::type_name::<T>());
        }
    }

    fn metrics_record_eviction(&self, reason: EventReason) {
        if let Some(m) = &self.inner.config.metrics {
            m.record_eviction(std::any::type_name::<T>(), reason);
        }
    }

    fn metrics_record_lru_size(&self, size: usize) {
        if let Some(m) = &self.inner.config.metrics {
            m.record_lru_size(std::any::type_name::<T>(), size);
        }
    }

    fn metrics_record_fetch_latency(&self, duration: Duration) {
        if let Some(m) = &self.inner.config.metrics {
            m.record_fetch_latency(std::any::type_name::<T>(), duration);
        }
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
    ///   reach the executor's `sleep(0)` in a tight loop. Use `None`
    ///   to disable the sweep.
    /// - `config.event_channel_capacity == 0` — would reach
    ///   `broadcast::channel(0)` and panic at runtime. The minimum
    ///   sensible value is `1`.
    /// - `config.namespace == Some(String::new())` — an empty string
    ///   would silently prefix L2 backend keys with a leading
    ///   separator and could collide with un-namespaced deployments.
    ///   Use `None` to disable namespacing.
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
            // Without a runtime feature, we have no spawn primitive
            // for the sweep task. Silently no-op'ing the user's opt-in
            // would mean expired entries never evict and the sweep's
            // promised events / metrics never fire — the spec would
            // be lying to the consumer. Fail loudly at build time
            // with a clear remediation pointer.
            #[cfg(not(any(feature = "runtime-tokio", feature = "runtime-wasm")))]
            {
                let _ = d;
                panic!(
                    "PunnuConfig::ttl_sweep_interval requires either the `runtime-tokio` \
                     or `runtime-wasm` feature. Without a runtime, sassi has no spawn \
                     primitive to drive the sweep task and silently discarding the \
                     opt-in would lie about TTL behavior. Either enable a runtime \
                     feature or set `ttl_sweep_interval` to `None` for lazy-only \
                     TTL expiry on `get`."
                );
            }
        }
        assert!(
            self.config.event_channel_capacity > 0,
            "PunnuConfig::event_channel_capacity must be greater than 0; \
             the broadcast channel rejects a zero capacity"
        );
        if let Some(ns) = &self.config.namespace {
            assert!(
                !ns.is_empty(),
                "PunnuConfig::namespace must be non-empty when set; \
                 use None to disable namespacing. An empty string would silently \
                 prefix L2 backend keys with a leading separator and could collide \
                 with un-namespaced deployments."
            );
        }
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
            in_flight: InFlightRegistry::new(),
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
