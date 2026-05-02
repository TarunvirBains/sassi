//! [`Punnu<T>`] — the typed in-memory pool.
//!
//! Holds `Arc<PunnuInner<T>>` so the public `Punnu<T>` is cheap to
//! clone and shareable across tasks; every clone observes the same
//! identity map, the same event stream, and the same configuration.
//!
//! # Concurrency
//!
//! L1 uses immutable snapshots published through [`arc_swap::ArcSwap`].
//! Reads load one snapshot and never coordinate with writers; writes
//! prepare a new snapshot under a small synchronous coordinator and
//! publish it atomically.

use crate::cacheable::Cacheable;
use crate::error::{FetchError, InsertError};
use crate::executor::{DefaultExecutor, PunnuExecutor};
use crate::predicate::MemQ;
use crate::punnu::config::{CacheTier, OnConflict, PunnuConfig, record_metric_safely};
use crate::punnu::delta::{DeltaApplyStats, DeltaResult};
use crate::punnu::events::{EventReason, InvalidationReason, PunnuEvent};
use crate::punnu::eviction::choose_sampled_lru_victim;
use crate::punnu::scope::PunnuScope;
use crate::punnu::single_flight::InFlightRegistry;
use crate::punnu::state::{Entry, L1State};
#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
use crate::punnu::ttl::spawn_sweep;
use crate::punnu::write::PreparedWrite;
use crate::time::Instant;
use arc_swap::ArcSwap;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
use tokio::sync::Notify;
use tokio::sync::broadcast;

#[cfg(test)]
type AfterPublishHook = Arc<dyn Fn() + Send + Sync + 'static>;

#[cfg(all(
    test,
    any(
        all(feature = "runtime-tokio", not(target_arch = "wasm32")),
        all(feature = "runtime-wasm", target_arch = "wasm32"),
    )
))]
struct SweepCandidatePause {
    collected: Arc<Notify>,
    resume: Arc<Notify>,
}

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
    /// Published L1 identity-map snapshot. Readers load this directly
    /// and never acquire the write coordinator.
    pub(crate) l1: ArcSwap<L1State<T>>,

    /// Serialises externally observable commits: snapshot publish,
    /// event broadcast, and metrics recording. Writers acquire this
    /// before `write_coord`, which prevents a later writer from
    /// publishing or emitting ahead of an earlier committed mutation.
    pub(crate) commit_coord: Mutex<()>,

    /// Serialises the short snapshot state write so each mutation
    /// prepares from the latest committed state and publishes exactly
    /// one successor.
    pub(crate) write_coord: Mutex<()>,

    /// Test-only hook fired after publishing a snapshot and before
    /// emitting its events. Unit tests use this to deterministically
    /// exercise commit/event ordering races without exposing a public
    /// synchronization surface.
    #[cfg(test)]
    after_publish_hook: Mutex<Option<AfterPublishHook>>,

    /// Monotonic access marker used by sampled-LRU eviction.
    pub(crate) access_clock: AtomicU64,

    /// RNG used only by snapshot writers when capacity pressure needs
    /// a sampled-LRU victim.
    pub(crate) eviction_rng: Mutex<fastrand::Rng>,

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
    #[cfg(any(
        all(feature = "runtime-tokio", not(target_arch = "wasm32")),
        all(feature = "runtime-wasm", target_arch = "wasm32"),
    ))]
    pub(crate) sweep_initialised: Option<Arc<Notify>>,

    /// Test-only pause point for the candidate-then-prepare sweep race.
    /// When set, the next sweep tick that collects at least one expired
    /// candidate notifies `collected` and waits on `resume` before it
    /// enters the coordinated prepare path.
    #[cfg(all(
        test,
        any(
            all(feature = "runtime-tokio", not(target_arch = "wasm32")),
            all(feature = "runtime-wasm", target_arch = "wasm32"),
        )
    ))]
    sweep_candidate_pause: Mutex<Option<SweepCandidatePause>>,

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
    /// Identity-map semantics: the entry is keyed by `value.id()`.
    /// Expired resident entries are treated as absent during the
    /// write, removed silently from the new snapshot, and do not
    /// trigger conflict handling or TTL events. If a non-expired
    /// entry with the same id is already cached, behaviour follows
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
    /// If the insert pushes the LRU past `lru_size`, sampled-LRU
    /// pressure evicts a resident entry and an
    /// [`EventReason::LruEvict`] event fires before the
    /// `Insert` / `Update` event.
    ///
    /// `async` because L2 backend write-through (a later task) is
    /// async; the L1-only path resolves immediately.
    ///
    /// # Errors
    ///
    /// - [`InsertError::Conflict`] if [`OnConflict::Reject`] is
    ///   configured and a non-expired entry with the id already
    ///   exists.
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
    /// for this entry without touching the pool's default; durations
    /// that exceed the target clock's representable range saturate to
    /// "no expiry."
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
        let expires_at = self.expiry_deadline(ttl);
        let epoch = self.next_access_epoch();
        self.with_write_coordinator(|state| self.prepare_insert(state, arc, expires_at, epoch))
    }

    /// Synchronous L1 lookup. Returns `Some(Arc<T>)` if the id is
    /// cached and unexpired; `None` if it isn't cached **or** the
    /// entry's TTL has elapsed.
    ///
    /// On hit, refreshes the entry's sampled-LRU epoch with one
    /// relaxed atomic store on the snapshot entry.
    ///
    /// # TTL semantics (lazy expiry path)
    ///
    /// If the entry exists but `expires_at <= Instant::now()`:
    ///
    /// `None` is returned. The expired entry is left in the published
    /// snapshot until a writer or the background sweep removes it.
    /// Lazy expiry records miss/eviction metrics but emits no event.
    pub fn get(&self, id: &T::Id) -> Option<Arc<T>> {
        let now = self.inner.executor.now();
        let snapshot = self.inner.l1.load();
        let Some(entry) = snapshot.get(id) else {
            self.metrics_record_miss();
            return None;
        };

        if entry.is_expired_at(now) {
            self.metrics_record_eviction(EventReason::TtlExpired);
            self.metrics_record_miss();
            return None;
        }

        entry.bump_access_epoch(self.next_access_epoch());
        self.metrics_record_hit(CacheTier::L1);
        Some(entry.value.clone())
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
    /// [`EventReason`] taxonomy so sassi-internal call sites (future
    /// backend-driven invalidation) can emit system-internal reasons
    /// without going through [`InvalidationReason`].
    ///
    /// Identical L1 semantics to [`Punnu::invalidate`] otherwise.
    /// `pub(crate)` so it cannot be used by external callers — that
    /// would defeat the public-vs-internal split that motivates the
    /// two enums.
    pub(crate) async fn invalidate_internal(&self, id: &T::Id, reason: EventReason) {
        let id = id.clone();
        self.with_write_coordinator(|state| self.prepare_invalidate(state, &id, reason));
    }

    /// Atomically apply a delta-sync batch to the L1 identity map.
    ///
    /// `items` are upserted by canonical id and `tombstones` are true
    /// deletes against the whole resident identity map. Absence from
    /// `items` never deletes a resident entry; use a tombstone for
    /// source-of-truth deletes. When the same id appears in both
    /// collections, the tombstone wins and the item is skipped before
    /// conflict checks, event emission, or `applied_items` accounting.
    ///
    /// The delta is prepared from one snapshot and published with one
    /// atomic snapshot store. Events are emitted only after the new
    /// snapshot is visible, using the same commit path as inserts and
    /// invalidations.
    ///
    /// Duplicate item ids within one delta are coalesced before conflict
    /// checks; the last item wins unless its id is tombstoned. Under
    /// [`OnConflict::Reject`], only the live entry from the pre-delta
    /// snapshot is considered a conflict, so duplicate ids inside the
    /// same delta do not reject each other. `applied_items` and
    /// insert/update events describe values that survive into the final
    /// published snapshot after sampled-LRU pressure. `lru_evictions`
    /// counts previously visible resident ids removed by sampled-LRU;
    /// transient delta candidates dropped before publication are not
    /// observable and are not counted.
    pub fn apply_delta(&self, delta: DeltaResult<T>) -> DeltaApplyStats {
        self.with_write_coordinator(|state| self.prepare_apply_delta(state, delta))
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
    /// # Canonical identity contract
    ///
    /// `get_or_fetch` is an identity fetch helper, not a query-result
    /// membership helper. The fetcher must resolve the requested
    /// canonical id. If it returns `Some(value)` where `value.id()`
    /// does not equal the requested id, Sassi returns
    /// [`FetchError::IdentityMismatch`] and does not cache the value.
    /// Auth-filtered, paginated, tenant-filtered, or query-specific
    /// fetchers should use a tenant-scoped `Punnu`, a distinct
    /// wrapper type, a deliberately tenant-qualified id type, or the
    /// refresh/subscription APIs that carry explicit query state.
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
        // re-run `insert_arc_or_existing` after the await, multiplying
        // events / TTL deadlines / OnConflict evaluations.
        //
        // `on_fetched` returns the canonical `Arc<T>`: the freshly-
        // fetched one on the success path, or the already-cached
        // value when a non-expired entry beat us to the slot during
        // the fetch. The atomic `insert_arc_or_existing` handles
        // both the "fresh insert" and "existing entry, return it"
        // cases under one coordinated snapshot write — no expired-
        // conflict race.
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
            .await;

        let elapsed = self.inner.executor.now() - start;
        self.metrics_record_fetch_latency(elapsed);

        result
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
    /// # Canonical identity contract
    ///
    /// `get_or_fetch_many` is a batch canonical-id fetch helper. The
    /// batch fetcher may return any subset of the requested missing
    /// ids, but every returned `T::id()` must be in that missing-id
    /// set. If the fetcher returns an unsolicited id, Sassi returns
    /// [`FetchError::IdentityMismatch`] before mutating L1. Duplicate
    /// returned ids are deduplicated deterministically before insert.
    /// Do not use this API to encode query/page/filter membership;
    /// use a tenant-scoped `Punnu`, a distinct wrapper type, a
    /// deliberately tenant-qualified id type, or the refresh APIs that
    /// carry explicit query state.
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
        let missing_set = seen_missing;
        let fetched = batch_fetcher(missing).await?;
        let mut deduped_fetched = Vec::with_capacity(fetched.len());
        let mut seen_returned: std::collections::HashSet<T::Id> = std::collections::HashSet::new();
        for value in fetched {
            let id = value.id();
            if !missing_set.contains(&id) {
                return Err(FetchError::IdentityMismatch {
                    type_name: std::any::type_name::<T>(),
                });
            }
            if seen_returned.insert(id) {
                deduped_fetched.push(value);
            }
        }

        if deduped_fetched.is_empty() {
            return Ok(hits);
        }

        let fetched_arcs = self.insert_many_for_fetch(deduped_fetched)?;
        hits.extend(fetched_arcs);
        Ok(hits)
    }

    /// Insert a pre-built `Arc<T>` into L1 without cloning the
    /// inner value. Used by [`Punnu::get_or_fetch`] after a
    /// single-flight fetch — the fetcher returns `T`, single-flight
    /// wraps it in an `Arc<T>` for cheap multi-awaiter sharing, and
    /// this method threads the arc into L1 using the
    /// single-flight-specific "insert if absent or expired, otherwise
    /// return existing" policy.
    ///
    /// `pub(crate)` because consumers should use `insert` (which
    /// handles boxing); this is the internal escape hatch for the
    /// single-flight path.
    /// Single-flight insert variant: insert `arc` into L1 if the slot
    /// is empty *or holds an expired entry*; otherwise return the
    /// already-cached non-expired `Arc<T>`. Atomic under the write
    /// coordinator and one snapshot publish — no expired-conflict race
    /// window.
    ///
    /// Used by [`Punnu::get_or_fetch`]'s `on_fetched` callback.
    /// Resolves the BLOCK-M2 corner case where the consumer's
    /// `OnConflict::Reject` policy would otherwise collide with an
    /// entry that is expired but still resident: this method treats
    /// expired entries as absent and inserts the freshly-fetched
    /// value, satisfying `get_or_fetch`'s documented post-condition
    /// that a subsequent `get` hits.
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
        let ttl = self.inner.config.default_ttl;
        let expires_at = self.expiry_deadline(ttl);
        let epoch = self.next_access_epoch();
        self.with_write_coordinator(|state| {
            self.prepare_insert_or_existing(state, arc, expires_at, epoch)
        })
    }

    fn insert_many_for_fetch(&self, values: Vec<T>) -> Result<Vec<Arc<T>>, InsertError> {
        let prepared_values = values
            .into_iter()
            .map(|value| {
                let ttl = self.inner.config.default_ttl;
                let expires_at = self.expiry_deadline(ttl);
                let epoch = self.next_access_epoch();
                (Arc::new(value), expires_at, epoch)
            })
            .collect::<Vec<_>>();

        self.with_write_coordinator(|state| self.prepare_insert_many(state, prepared_values))
    }

    /// Number of entries currently in the L1.
    ///
    /// Snapshots the current size; concurrent inserts / invalidates
    /// against another `Punnu<T>` clone may change the value before
    /// the caller reads it. Suitable for diagnostics and tests, not
    /// for "is the entry I just inserted definitely visible?" checks
    /// — use `get` for that.
    pub fn len(&self) -> usize {
        self.inner.l1.load().len()
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
    /// does not perform TTL cleanup or emit invalidation events. The
    /// public `get` path has the same no-cleanup lazy-expiry
    /// contract; physical removal is left to writers or the
    /// background sweep.
    pub(crate) fn snapshot_unexpired(&self) -> Vec<Arc<T>> {
        let now = self.inner.executor.now();
        let snapshot = self.inner.l1.load_full();
        snapshot
            .entries
            .iter()
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
    #[cfg(any(
        all(feature = "runtime-tokio", not(target_arch = "wasm32")),
        all(feature = "runtime-wasm", target_arch = "wasm32"),
    ))]
    #[doc(hidden)]
    pub fn _test_sweep_initialised(&self) -> Option<Arc<Notify>> {
        self.inner.sweep_initialised.clone()
    }

    #[cfg(all(
        test,
        any(
            all(feature = "runtime-tokio", not(target_arch = "wasm32")),
            all(feature = "runtime-wasm", target_arch = "wasm32"),
        )
    ))]
    fn pause_next_sweep_after_candidates_for_test(
        &self,
        collected: Arc<Notify>,
        resume: Arc<Notify>,
    ) {
        *self
            .inner
            .sweep_candidate_pause
            .lock()
            .expect("Punnu sweep-candidate test hook lock poisoned") =
            Some(SweepCandidatePause { collected, resume });
    }

    fn with_write_coordinator<R>(
        &self,
        prepare: impl FnOnce(L1State<T>) -> PreparedWrite<T, R>,
    ) -> R {
        let _commit_guard = self.inner.commit_coord.lock().expect(
            "Punnu L1 commit coordinator poisoned — a previous panic interrupted event emission",
        );
        let (events, result, post_len) = {
            let _guard = self.inner.write_coord.lock().expect(
                "Punnu L1 write coordinator poisoned — a previous panic interrupted a write",
            );
            let current = self.inner.l1.load_full();
            let prepared = prepare((*current).clone());
            #[cfg(debug_assertions)]
            prepared.state().assert_invariants();
            let post_len = prepared.state().len();
            let (state, events, result) = prepared.into_parts();
            self.inner.l1.store(Arc::new(state));
            (events, result, post_len)
        };

        #[cfg(test)]
        self.run_after_publish_hook();
        self.emit_committed_events(events, post_len);
        result
    }

    #[cfg(test)]
    fn set_after_publish_hook(&self, hook: Option<AfterPublishHook>) {
        *self
            .inner
            .after_publish_hook
            .lock()
            .expect("Punnu after-publish test hook lock poisoned") = hook;
    }

    #[cfg(test)]
    fn run_after_publish_hook(&self) {
        let hook = self
            .inner
            .after_publish_hook
            .lock()
            .expect("Punnu after-publish test hook lock poisoned")
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    fn prepare_insert(
        &self,
        mut state: L1State<T>,
        value: Arc<T>,
        expires_at: Option<Instant>,
        epoch: u64,
    ) -> PreparedWrite<T, Result<Arc<T>, InsertError>> {
        let id = value.id();
        let now = self.inner.executor.now();
        Self::remove_expired_entry(&mut state, &id, now);

        let existing = state.get(&id).cloned();
        if existing.is_some() && matches!(self.inner.config.on_conflict, OnConflict::Reject) {
            return PreparedWrite::new(state, Vec::new(), Err(InsertError::Conflict));
        }

        let entry = Arc::new(Entry::new(value.clone(), expires_at, epoch));
        state.insert_entry(id.clone(), entry);

        let write_event = match (existing, self.inner.config.on_conflict) {
            (Some(old), OnConflict::Update) => PunnuEvent::Update {
                old: old.value.clone(),
                new: value.clone(),
            },
            _ => PunnuEvent::Insert {
                value: value.clone(),
            },
        };

        let mut events = Vec::new();
        self.remove_expired_entries_for_capacity(&mut state, now);
        self.evict_to_capacity(&mut state, &mut events, Some(&id));
        events.push(write_event);

        PreparedWrite::new(state, events, Ok(value))
    }

    fn prepare_insert_many(
        &self,
        mut state: L1State<T>,
        values: Vec<(Arc<T>, Option<Instant>, u64)>,
    ) -> PreparedWrite<T, Result<Vec<Arc<T>>, InsertError>> {
        let now = self.inner.executor.now();
        let original_state = state.clone();
        let mut events = Vec::new();
        let mut accepted = Vec::with_capacity(values.len());

        for (value, expires_at, epoch) in values {
            let id = value.id();
            Self::remove_expired_entry(&mut state, &id, now);

            let existing = state.get(&id).cloned();
            if existing.is_some() && matches!(self.inner.config.on_conflict, OnConflict::Reject) {
                return PreparedWrite::new(original_state, Vec::new(), Err(InsertError::Conflict));
            }

            let entry = Arc::new(Entry::new(value.clone(), expires_at, epoch));
            state.insert_entry(id.clone(), entry);
            accepted.push((id, value, existing.map(|entry| entry.value.clone())));
        }

        self.remove_expired_entries_for_capacity(&mut state, now);
        let lru_victims = self.evict_ids_to_capacity(&mut state, None);

        for (id, value, old) in &accepted {
            if state.get(id).is_none() {
                continue;
            }
            events.push(match (old, self.inner.config.on_conflict) {
                (Some(old), OnConflict::Update) => PunnuEvent::Update {
                    old: old.clone(),
                    new: value.clone(),
                },
                _ => PunnuEvent::Insert {
                    value: value.clone(),
                },
            });
        }

        for id in lru_victims {
            let was_visible = original_state
                .get(&id)
                .is_some_and(|entry| !entry.is_expired_at(now));
            if was_visible {
                events.push(PunnuEvent::Invalidate {
                    id,
                    reason: EventReason::LruEvict,
                });
            }
        }

        let inserted = accepted
            .into_iter()
            .map(|(_id, value, _old)| value)
            .collect();

        PreparedWrite::new(state, events, Ok(inserted))
    }

    fn prepare_insert_or_existing(
        &self,
        mut state: L1State<T>,
        value: Arc<T>,
        expires_at: Option<Instant>,
        epoch: u64,
    ) -> PreparedWrite<T, Arc<T>> {
        let id = value.id();
        let now = self.inner.executor.now();
        Self::remove_expired_entry(&mut state, &id, now);

        if let Some(existing) = state.get(&id).map(|entry| entry.value.clone()) {
            return PreparedWrite::new(state, Vec::new(), existing);
        }

        let entry = Arc::new(Entry::new(value.clone(), expires_at, epoch));
        state.insert_entry(id.clone(), entry);

        let mut events = Vec::new();
        self.remove_expired_entries_for_capacity(&mut state, now);
        self.evict_to_capacity(&mut state, &mut events, Some(&id));
        events.push(PunnuEvent::Insert {
            value: value.clone(),
        });

        PreparedWrite::new(state, events, value)
    }

    fn prepare_invalidate(
        &self,
        mut state: L1State<T>,
        id: &T::Id,
        reason: EventReason,
    ) -> PreparedWrite<T, bool> {
        let now = self.inner.executor.now();
        if Self::remove_expired_entry(&mut state, id, now) {
            return PreparedWrite::new(state, Vec::new(), false);
        }

        let removed = state.remove_entry(id).is_some();
        let events = if removed {
            vec![PunnuEvent::Invalidate {
                id: id.clone(),
                reason,
            }]
        } else {
            Vec::new()
        };

        PreparedWrite::new(state, events, removed)
    }

    fn prepare_apply_delta(
        &self,
        mut state: L1State<T>,
        delta: DeltaResult<T>,
    ) -> PreparedWrite<T, DeltaApplyStats> {
        let now = self.inner.executor.now();
        let original_state = state.clone();
        let mut events = Vec::new();
        let mut stats = DeltaApplyStats::default();
        let DeltaResult { items, tombstones } = delta;
        let mut normalized_items = BTreeMap::new();

        for value in items {
            let id = value.id();
            if !tombstones.contains(&id) {
                normalized_items.insert(id, value);
            }
        }

        let mut accepted_items = BTreeMap::new();

        for (id, value) in normalized_items {
            Self::remove_expired_entry(&mut state, &id, now);

            let existing = state.get(&id).cloned();
            if existing.is_some() && matches!(self.inner.config.on_conflict, OnConflict::Reject) {
                continue;
            }

            let value = Arc::new(value);
            let expires_at = self.expiry_deadline(self.inner.config.default_ttl);
            let epoch = self.next_access_epoch();
            state.insert_entry(
                id.clone(),
                Arc::new(Entry::new(value.clone(), expires_at, epoch)),
            );
            accepted_items.insert(id, existing.map(|entry| entry.value.clone()));
        }

        let mut tombstone_events = Vec::new();
        for id in tombstones {
            if Self::remove_expired_entry(&mut state, &id, now) {
                continue;
            }
            if state.remove_entry(&id).is_some() {
                stats.tombstones_evicted += 1;
                tombstone_events.push(PunnuEvent::Invalidate {
                    id,
                    reason: EventReason::OnDelete,
                });
            }
        }

        self.remove_expired_entries_for_capacity(&mut state, now);
        let lru_victims = self.evict_ids_to_capacity(&mut state, None);

        for (id, old) in accepted_items {
            let Some(final_entry) = state.get(&id) else {
                continue;
            };
            let value = final_entry.value.clone();
            stats.applied_items += 1;
            events.push(match (old, self.inner.config.on_conflict) {
                (Some(old), OnConflict::Update) => PunnuEvent::Update { old, new: value },
                _ => PunnuEvent::Insert { value },
            });
        }

        events.extend(tombstone_events);

        for id in lru_victims {
            let was_visible = original_state
                .get(&id)
                .is_some_and(|entry| !entry.is_expired_at(now));
            if was_visible {
                stats.lru_evictions += 1;
                events.push(PunnuEvent::Invalidate {
                    id,
                    reason: EventReason::LruEvict,
                });
            }
        }

        PreparedWrite::new(state, events, stats)
    }

    fn remove_expired_entry(state: &mut L1State<T>, id: &T::Id, now: Instant) -> bool {
        let expired = state.get(id).is_some_and(|entry| entry.is_expired_at(now));
        if expired {
            state.remove_entry(id);
        }
        expired
    }

    fn remove_expired_entries_for_capacity(&self, state: &mut L1State<T>, now: Instant) {
        if state.len() <= self.inner.config.lru_size {
            return;
        }

        let expired_ids = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.is_expired_at(now))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();

        for id in expired_ids {
            state.remove_entry(&id);
        }
    }

    fn evict_to_capacity(
        &self,
        state: &mut L1State<T>,
        events: &mut Vec<PunnuEvent<T>>,
        protected_id: Option<&T::Id>,
    ) -> usize {
        let victim_ids = self.evict_ids_to_capacity(state, protected_id);
        let evictions = victim_ids.len();
        events.extend(victim_ids.into_iter().map(|id| PunnuEvent::Invalidate {
            id,
            reason: EventReason::LruEvict,
        }));
        evictions
    }

    fn evict_ids_to_capacity(
        &self,
        state: &mut L1State<T>,
        protected_id: Option<&T::Id>,
    ) -> Vec<T::Id> {
        let mut victim_ids = Vec::new();
        while state.len() > self.inner.config.lru_size {
            let Some(victim_id) = self.choose_eviction_victim(state, protected_id) else {
                break;
            };
            state.remove_entry(&victim_id);
            victim_ids.push(victim_id);
        }
        victim_ids
    }

    fn choose_eviction_victim(
        &self,
        state: &L1State<T>,
        protected_id: Option<&T::Id>,
    ) -> Option<T::Id> {
        let sampled = {
            let mut rng = self
                .inner
                .eviction_rng
                .lock()
                .expect("Punnu L1 eviction RNG lock poisoned");
            choose_sampled_lru_victim(state, &mut rng)
        };

        match sampled {
            Some(id) if Self::is_protected(protected_id, &id) && state.len() > 1 => state
                .entries
                .iter()
                .filter(|(candidate_id, _)| !Self::is_protected(protected_id, candidate_id))
                .min_by_key(|(_, entry)| entry.access_epoch())
                .map(|(candidate_id, _)| candidate_id.clone()),
            other => other,
        }
    }

    fn is_protected(protected_id: Option<&T::Id>, id: &T::Id) -> bool {
        matches!(protected_id, Some(protected) if protected == id)
    }

    fn expiry_deadline(&self, ttl: Option<Duration>) -> Option<Instant> {
        ttl.and_then(|d| self.inner.executor.now().checked_add(d))
    }

    fn emit_committed_events(&self, events: Vec<PunnuEvent<T>>, post_len: usize) {
        let mut record_lru_size = false;
        for event in events {
            let eviction_reason = match &event {
                PunnuEvent::Invalidate { reason, .. } => Some(*reason),
                PunnuEvent::Insert { .. } | PunnuEvent::Update { .. } => None,
            };
            record_lru_size = true;

            let _ = self.inner.events.send(event);
            if let Some(reason) = eviction_reason {
                self.metrics_record_eviction(reason);
            }
        }

        if record_lru_size {
            self.metrics_record_lru_size(post_len);
        }
    }

    fn next_access_epoch(&self) -> u64 {
        self.inner
            .access_clock
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
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
            record_metric_safely(|| m.record_hit(std::any::type_name::<T>(), tier));
        }
    }

    fn metrics_record_miss(&self) {
        if let Some(m) = &self.inner.config.metrics {
            record_metric_safely(|| m.record_miss(std::any::type_name::<T>()));
        }
    }

    fn metrics_record_eviction(&self, reason: EventReason) {
        if let Some(m) = &self.inner.config.metrics {
            record_metric_safely(|| m.record_eviction(std::any::type_name::<T>(), reason));
        }
    }

    fn metrics_record_lru_size(&self, size: usize) {
        if let Some(m) = &self.inner.config.metrics {
            record_metric_safely(|| m.record_lru_size(std::any::type_name::<T>(), size));
        }
    }

    fn metrics_record_fetch_latency(&self, duration: Duration) {
        if let Some(m) = &self.inner.config.metrics {
            record_metric_safely(|| m.record_fetch_latency(std::any::type_name::<T>(), duration));
        }
    }
}

#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
impl<T: Cacheable> PunnuInner<T> {
    /// Prepare and publish a TTL sweep from candidate ids collected
    /// from an earlier snapshot.
    ///
    /// Returns `false` only when a coordinator lock was poisoned; the
    /// sweep task treats that as a terminal condition and exits.
    pub(crate) fn sweep_expired(&self, candidate_ids: Vec<T::Id>) -> bool {
        let _commit_guard = match self.commit_coord.lock() {
            Ok(guard) => guard,
            Err(_) => return false,
        };

        let (removed_ids, post_len) = {
            let _guard = match self.write_coord.lock() {
                Ok(guard) => guard,
                Err(_) => return false,
            };
            let now = self.executor.now();
            let mut state = (*self.l1.load_full()).clone();
            let mut removed_ids = Vec::new();

            for id in candidate_ids {
                if state.get(&id).is_some_and(|entry| entry.is_expired_at(now)) {
                    state.remove_entry(&id);
                    removed_ids.push(id);
                }
            }

            if removed_ids.is_empty() {
                (removed_ids, state.len())
            } else {
                #[cfg(debug_assertions)]
                state.assert_invariants();
                let post_len = state.len();
                self.l1.store(Arc::new(state));
                (removed_ids, post_len)
            }
        };

        let removed_count = removed_ids.len();
        for id in removed_ids {
            let _ = self.events.send(PunnuEvent::Invalidate {
                id,
                reason: EventReason::TtlExpired,
            });
            if let Some(m) = &self.config.metrics {
                record_metric_safely(|| {
                    m.record_eviction(std::any::type_name::<T>(), EventReason::TtlExpired);
                });
            }
        }

        if removed_count > 0
            && let Some(m) = &self.config.metrics
        {
            record_metric_safely(|| m.record_lru_size(std::any::type_name::<T>(), post_len));
        }

        true
    }

    #[cfg(all(
        test,
        any(
            all(feature = "runtime-tokio", not(target_arch = "wasm32")),
            all(feature = "runtime-wasm", target_arch = "wasm32"),
        )
    ))]
    pub(crate) async fn pause_next_sweep_after_candidates_for_test(&self, has_candidates: bool) {
        if !has_candidates {
            return;
        }

        let pause = {
            let mut hook = self
                .sweep_candidate_pause
                .lock()
                .expect("Punnu sweep-candidate test hook lock poisoned");
            hook.take()
        };

        if let Some(pause) = pause {
            pause.collected.notify_one();
            pause.resume.notified().await;
        }
    }
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
    /// background task that scans the L1 every interval tick for
    /// TTL-expired entries (gated behind `runtime-tokio` on native
    /// builds or `runtime-wasm` on WASM builds). The sweep task uses
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
        if self.config.lru_size == 0 {
            panic!(
                "PunnuConfig::lru_size must be non-zero (got 0); \
                 a zero-capacity LRU evicts every insert immediately"
            )
        }
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
            #[cfg(not(any(
                all(feature = "runtime-tokio", not(target_arch = "wasm32")),
                all(feature = "runtime-wasm", target_arch = "wasm32"),
            )))]
            {
                let _ = d;
                panic!(
                    "PunnuConfig::ttl_sweep_interval requires `runtime-tokio` on native \
                     targets or `runtime-wasm` on wasm32. Without a target-compatible \
                     runtime, sassi has no spawn primitive to drive the sweep task and \
                     silently discarding the opt-in would lie about TTL behavior. Enable \
                     the runtime feature for this target or set `ttl_sweep_interval` to \
                     `None` for lazy-only TTL expiry on `get`."
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

        #[cfg(any(
            all(feature = "runtime-tokio", not(target_arch = "wasm32")),
            all(feature = "runtime-wasm", target_arch = "wasm32"),
        ))]
        let sweep_initialised = sweep_interval.map(|_| Arc::new(Notify::new()));

        let inner = Arc::new(PunnuInner {
            l1: ArcSwap::from_pointee(L1State::empty()),
            commit_coord: Mutex::new(()),
            write_coord: Mutex::new(()),
            #[cfg(test)]
            after_publish_hook: Mutex::new(None),
            access_clock: AtomicU64::new(0),
            eviction_rng: Mutex::new(fastrand::Rng::new()),
            events,
            config: self.config,
            executor,
            #[cfg(any(
                all(feature = "runtime-tokio", not(target_arch = "wasm32")),
                all(feature = "runtime-wasm", target_arch = "wasm32"),
            ))]
            sweep_initialised: sweep_initialised.clone(),
            #[cfg(all(
                test,
                any(
                    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
                    all(feature = "runtime-wasm", target_arch = "wasm32"),
                )
            ))]
            sweep_candidate_pause: Mutex::new(None),
            in_flight: InFlightRegistry::new(),
        });

        // Spawn the sweep before constructing the public handle so
        // the weak ref is captured against the same `Arc` we hand
        // back. With both runtime features off, the call site is a no-op.
        #[cfg(any(
            all(feature = "runtime-tokio", not(target_arch = "wasm32")),
            all(feature = "runtime-wasm", target_arch = "wasm32"),
        ))]
        spawn_sweep_if_configured(&inner, sweep_interval, sweep_initialised);
        #[cfg(not(any(
            all(feature = "runtime-tokio", not(target_arch = "wasm32")),
            all(feature = "runtime-wasm", target_arch = "wasm32"),
        )))]
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
#[cfg(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
))]
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

#[cfg(test)]
mod commit_order_tests {
    use super::*;
    use crate::cacheable::Field;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Condvar, Mutex as StdMutex};
    use std::thread;
    use std::time::{Duration as StdDuration, Instant as StdInstant};
    #[cfg(all(feature = "runtime-tokio", not(target_arch = "wasm32")))]
    use tokio::sync::Notify;
    use tokio::sync::broadcast::error::TryRecvError;

    #[derive(Debug, Clone)]
    struct EventOrderItem {
        id: i64,
    }

    #[derive(Default)]
    struct EventOrderFields {
        _id: Field<EventOrderItem, i64>,
    }

    impl Cacheable for EventOrderItem {
        type Id = i64;
        type Fields = EventOrderFields;

        fn id(&self) -> Self::Id {
            self.id
        }

        fn fields() -> Self::Fields {
            EventOrderFields {
                _id: Field::new("id", |item| &item.id),
            }
        }
    }

    struct Gate {
        state: StdMutex<GateState>,
        cvar: Condvar,
    }

    struct GateState {
        entered: bool,
        release: bool,
    }

    impl Gate {
        fn new() -> Self {
            Self {
                state: StdMutex::new(GateState {
                    entered: false,
                    release: false,
                }),
                cvar: Condvar::new(),
            }
        }

        fn enter_and_wait(&self) {
            let mut state = self.state.lock().expect("gate lock poisoned");
            state.entered = true;
            self.cvar.notify_all();
            while !state.release {
                state = self.cvar.wait(state).expect("gate lock poisoned");
            }
        }

        fn wait_until_entered(&self) {
            let mut state = self.state.lock().expect("gate lock poisoned");
            while !state.entered {
                state = self.cvar.wait(state).expect("gate lock poisoned");
            }
        }

        fn release(&self) {
            let mut state = self.state.lock().expect("gate lock poisoned");
            state.release = true;
            self.cvar.notify_all();
        }
    }

    fn insert_event_id(event: PunnuEvent<EventOrderItem>) -> i64 {
        match event {
            PunnuEvent::Insert { value } => value.id,
            other => panic!("expected insert event, got {other:?}"),
        }
    }

    #[test]
    fn commit_events_are_emitted_before_a_later_writer_can_publish() {
        let punnu = Punnu::<EventOrderItem>::builder().build();
        let mut rx = punnu.events();
        let gate = Arc::new(Gate::new());
        let first_publish = Arc::new(AtomicBool::new(true));

        let gate_for_hook = gate.clone();
        let first_publish_for_hook = first_publish.clone();
        punnu.set_after_publish_hook(Some(Arc::new(move || {
            if first_publish_for_hook.swap(false, Ordering::SeqCst) {
                gate_for_hook.enter_and_wait();
            }
        })));

        let punnu_for_first = punnu.clone();
        let first = thread::spawn(move || {
            futures::executor::block_on(async {
                punnu_for_first
                    .insert(EventOrderItem { id: 1 })
                    .await
                    .unwrap();
            });
        });

        gate.wait_until_entered();

        let second_finished = Arc::new(AtomicBool::new(false));
        let punnu_for_second = punnu.clone();
        let second_finished_for_thread = second_finished.clone();
        let second = thread::spawn(move || {
            futures::executor::block_on(async {
                punnu_for_second
                    .insert(EventOrderItem { id: 2 })
                    .await
                    .unwrap();
            });
            second_finished_for_thread.store(true, Ordering::SeqCst);
        });

        let deadline = StdInstant::now() + StdDuration::from_millis(100);
        while !second_finished.load(Ordering::SeqCst) && StdInstant::now() < deadline {
            thread::yield_now();
        }

        assert!(
            !second_finished.load(Ordering::SeqCst),
            "later writer completed while an earlier publish was still awaiting event emission"
        );
        assert!(
            matches!(rx.try_recv(), Err(TryRecvError::Empty)),
            "no event should be observable before the earliest committed writer emits"
        );

        gate.release();
        first.join().expect("first writer thread panicked");
        second.join().expect("second writer thread panicked");

        assert_eq!(insert_event_id(rx.try_recv().unwrap()), 1);
        assert_eq!(insert_event_id(rx.try_recv().unwrap()), 2);
    }

    #[cfg(all(feature = "runtime-tokio", not(target_arch = "wasm32")))]
    #[tokio::test(start_paused = true)]
    async fn sweep_rechecks_expiry_before_removing_stale_candidate() {
        let punnu = Punnu::<EventOrderItem>::builder()
            .config(PunnuConfig {
                default_ttl: Some(Duration::from_secs(5)),
                ttl_sweep_interval: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .build();
        let mut rx = punnu.events();

        punnu.insert(EventOrderItem { id: 1 }).await.unwrap();
        let notify = punnu
            ._test_sweep_initialised()
            .expect("tokio sweep should expose readiness hook in tests");
        notify.notified().await;

        let candidates_collected = Arc::new(Notify::new());
        let resume_prepare = Arc::new(Notify::new());
        punnu.pause_next_sweep_after_candidates_for_test(
            candidates_collected.clone(),
            resume_prepare.clone(),
        );

        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        candidates_collected.notified().await;

        punnu
            .insert_with_ttl(EventOrderItem { id: 1 }, Duration::from_secs(60))
            .await
            .expect("newer same-id write should commit before sweep prepare resumes");

        resume_prepare.notify_one();
        for _ in 0..2 {
            tokio::task::yield_now().await;
        }

        assert!(
            punnu.get(&1).is_some(),
            "sweep must not evict a newer same-id value that replaced an expired candidate"
        );
        assert_eq!(punnu.len(), 1);

        while let Ok(ev) = rx.try_recv() {
            assert!(
                !matches!(
                    ev,
                    PunnuEvent::Invalidate {
                        id: 1,
                        reason: EventReason::TtlExpired,
                    }
                ),
                "stale candidate should not emit a TtlExpired event for the newer value"
            );
        }
    }
}
