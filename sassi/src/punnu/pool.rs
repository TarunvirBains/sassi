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
use crate::punnu::config::{OnConflict, PunnuConfig};
use crate::punnu::events::{InvalidationReason, PunnuEvent};
use crate::punnu::ttl::Entry;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
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
/// 2. [`Punnu::insert`] entries (or via [`Punnu::insert_with_ttl`] in
///    a later task — pinned in the trait now for forward
///    compatibility; the method ships in the TTL task).
/// 3. [`Punnu::get`] entries by id (synchronous).
/// 4. [`Punnu::invalidate`] entries explicitly, or let LRU / TTL
///    pressure evict them.
/// 5. Subscribe to [`Punnu::events`] for an observability stream.
///
/// Drop the last `Punnu<T>` clone to release the inner state — any
/// background sweep task (Task 6) checks the strong count via
/// `Arc::downgrade` and exits cleanly.
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
    /// [`InvalidationReason::LruEvict`] event fires before the
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
    pub async fn insert(&self, value: T) -> Result<Arc<T>, InsertError> {
        let arc = Arc::new(value);
        self.insert_arc_internal(arc).await
    }

    /// Internal insert — shared by `insert` (and, in Task 6,
    /// `insert_with_ttl`).
    async fn insert_arc_internal(&self, arc: Arc<T>) -> Result<Arc<T>, InsertError> {
        let id = arc.id();
        let entry = Entry::new(arc.clone());

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
                reason: InvalidationReason::LruEvict,
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
    /// cached, `None` if it isn't.
    ///
    /// On hit, refreshes the entry's LRU recency. (`LruCache::get`
    /// takes `&mut self` precisely because it updates the recency
    /// list — that's why this method takes the write lock even
    /// though it's read-shaped at the API level. See module-level
    /// docs.)
    ///
    /// TTL-aware lazy expiry wires up in a later task; for now an
    /// expired entry is still served (the storage layer carries
    /// `expires_at` from day one, but the check itself lands with
    /// the TTL behaviour).
    pub fn get(&self, id: &T::Id) -> Option<Arc<T>> {
        let mut map =
            self.inner.map.write().expect(
                "Punnu L1 lock poisoned — a previous panic left it in an inconsistent state",
            );
        map.get(id).map(|entry| entry.value.clone())
    }

    /// Drop a single entry by id. No-op if the id is not cached.
    /// Emits [`PunnuEvent::Invalidate`] with the supplied reason
    /// when an entry was actually removed.
    ///
    /// `async` because L2 backend invalidation (a later task) is
    /// async; the L1-only path resolves immediately.
    pub async fn invalidate(&self, id: &T::Id, reason: InvalidationReason) {
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
    /// # Panics
    ///
    /// Panics if `config.lru_size == 0` — a zero-capacity LRU is a
    /// programmer error, not a runtime failure mode (every insert
    /// would immediately evict itself).
    pub fn build(self) -> Punnu<T> {
        let cap = NonZeroUsize::new(self.config.lru_size).unwrap_or_else(|| {
            panic!(
                "PunnuConfig::lru_size must be non-zero (got 0); \
                 a zero-capacity LRU evicts every insert immediately"
            )
        });
        let (events, _) = broadcast::channel(self.config.event_channel_capacity);
        Punnu {
            inner: Arc::new(PunnuInner {
                map: RwLock::new(LruCache::new(cap)),
                events,
                config: self.config,
            }),
        }
    }
}

impl<T: Cacheable> Default for PunnuBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}
