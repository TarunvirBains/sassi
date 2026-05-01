//! [`PunnuEvent`] — the event type broadcast over the
//! [`crate::punnu::Punnu`] event stream.
//!
//! Events are **lossy by design**: when a subscriber falls behind the
//! configured channel capacity ([`crate::punnu::PunnuConfig::event_channel_capacity`],
//! default 256), the backing
//! [`tokio::sync::broadcast`] channel drops the oldest events for that
//! subscriber and surfaces a `RecvError::Lagged` on the next receive.
//! The producer (the `Punnu` itself) never blocks and never errors
//! when receivers lag — slow subscribers degrade to skipped events,
//! the cache itself stays healthy.
//!
//! The same lossy contract holds when there are zero subscribers: a
//! `send` with no receivers returns `Err(SendError)` which the Punnu
//! ignores. Events are best-effort observability, not a durable log.

use crate::cacheable::Cacheable;
use std::sync::Arc;

/// A single observable event from a [`crate::punnu::Punnu`].
///
/// Generic over the cached type `T` so subscribers can match on the
/// payload without boxing. Carries `Arc<T>` for `Insert` / `Update` so
/// consumers can hold a reference to the entry without copying it.
///
/// `PunnuEvent<T>` does **not** require `T: Debug` — `T` is often a
/// payload type that doesn't derive `Debug`. Subscribers that want
/// debug output should match on the variant and format their own
/// fields.
///
/// # Reason taxonomy: [`EventReason`] vs. [`InvalidationReason`]
///
/// [`PunnuEvent::Invalidate`] carries an [`EventReason`] — the full
/// taxonomy including system-internal reasons ([`EventReason::LruEvict`],
/// [`EventReason::TtlExpired`], [`EventReason::BackendInvalidation`])
/// that the runtime constructs but callers cannot synthesise.
/// [`crate::punnu::Punnu::invalidate`] takes the narrower
/// [`InvalidationReason`] enum, which is the public subset
/// (`Manual` / `OnSave` / `OnDelete`). The split keeps the call-side
/// API honest — callers can't pass `LruEvict` to `invalidate` — while
/// subscribers continue to see one unified reason discriminator on the
/// event stream.
pub enum PunnuEvent<T: Cacheable> {
    /// A new entry landed in the cache. With
    /// [`crate::punnu::OnConflict::LastWriteWins`] (default) this fires
    /// for both first-insert and replace; with
    /// [`crate::punnu::OnConflict::Update`], replaces produce
    /// [`PunnuEvent::Update`] instead and `Insert` fires only on
    /// first-insert.
    Insert {
        /// Newly-cached value.
        value: Arc<T>,
    },

    /// An existing entry was replaced under
    /// [`crate::punnu::OnConflict::Update`].
    /// Carries both the previous and the new value so subscribers can
    /// diff them; the new value is also reachable via
    /// `punnu.get(&id)` after the event fires.
    Update {
        /// Previous cached value, just unseated.
        old: Arc<T>,
        /// Replacement value, now live in the cache.
        new: Arc<T>,
    },

    /// An entry left the cache. Carries the id and the reason so
    /// subscribers can distinguish manual invalidation, LRU eviction,
    /// and TTL expiry — common patterns (e.g., metrics, distributed
    /// invalidation fan-out) want to react differently per reason.
    ///
    /// `reason` is an [`EventReason`] (the full taxonomy) rather than
    /// an [`InvalidationReason`] (the public subset accepted by
    /// [`crate::punnu::Punnu::invalidate`]). System-internal reasons
    /// like [`EventReason::LruEvict`] and [`EventReason::TtlExpired`]
    /// are reachable here even though no caller can construct them.
    Invalidate {
        /// Id of the entry that left.
        id: T::Id,
        /// Why the entry left — full taxonomy.
        reason: EventReason,
    },
}

// `Clone` is required by `tokio::sync::broadcast` for the message
// type. We implement it manually rather than deriving so the bound
// remains `T: Cacheable` (without forcing `T: Clone` — `Arc<T>` is
// already `Clone` regardless of `T`).
impl<T: Cacheable> Clone for PunnuEvent<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Insert { value } => Self::Insert {
                value: value.clone(),
            },
            Self::Update { old, new } => Self::Update {
                old: old.clone(),
                new: new.clone(),
            },
            Self::Invalidate { id, reason } => Self::Invalidate {
                id: id.clone(),
                reason: *reason,
            },
        }
    }
}

// Manual `Debug` so the event type is debuggable without requiring
// `T: Debug` (id is always Debug-renderable via the trait bound below;
// we use the type name for the value placeholder rather than the
// payload itself, which matches the spec's "type_name as a metrics
// label" pattern).
impl<T: Cacheable> std::fmt::Debug for PunnuEvent<T>
where
    T::Id: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insert { .. } => f
                .debug_struct("PunnuEvent::Insert")
                .field("value_ty", &std::any::type_name::<T>())
                .finish(),
            Self::Update { .. } => f
                .debug_struct("PunnuEvent::Update")
                .field("value_ty", &std::any::type_name::<T>())
                .finish(),
            Self::Invalidate { id, reason } => f
                .debug_struct("PunnuEvent::Invalidate")
                .field("id", id)
                .field("reason", reason)
                .finish(),
        }
    }
}

/// Caller-supplied invalidation reason — the public subset accepted by
/// [`crate::punnu::Punnu::invalidate`].
///
/// Externally-initiated invalidation only. System-internal reasons
/// (LRU eviction, TTL expiry, backend-driven invalidation) live in the
/// sibling [`EventReason`] enum; callers cannot synthesise those.
///
/// The variants are stable wire-level identifiers — distributed
/// backends fan invalidations across processes by reason, so adding a
/// new variant is a minor-version event and removing one is a
/// breaking change. Sassi reserves the right to add new variants in
/// future minor releases (the type is `#[non_exhaustive]`).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationReason {
    /// Caller explicitly invalidated the entry — e.g.,
    /// `punnu.invalidate(&id, InvalidationReason::Manual).await`.
    Manual,

    /// Driven by a successful `Model::save` on the bound
    /// `DjogiContext` (the same-process invalidation path described
    /// in spec §6.1). Sassi-side semantics: the consumer surfaces this
    /// reason; sassi records it.
    OnSave,

    /// Driven by a successful `Model::delete` on the bound
    /// `DjogiContext` (spec §6.1).
    OnDelete,
}

/// Sealed marker for [`EventReason`]'s system-internal variants.
///
/// Held as the payload of [`EventReason::LruEvict`] /
/// [`EventReason::TtlExpired`] / [`EventReason::BackendInvalidation`].
/// The struct itself is `pub` so external code can pattern-match on the
/// containing variants (`EventReason::LruEvict(_)` requires the field
/// type to be visible). Construction is sealed because the inner `()`
/// is a private positional field — `Internal(())` requires a private
/// field, which external code can't write. Use [`Internal::new`]
/// (`pub(crate)`) inside sassi.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Internal(());

impl Internal {
    /// Internal constructor — sassi-only.
    pub(crate) const fn new() -> Self {
        Self(())
    }
}

/// Full invalidation taxonomy emitted on [`PunnuEvent::Invalidate`].
///
/// Includes both the public reasons callers can pass to
/// [`crate::punnu::Punnu::invalidate`] (lifted in via the
/// [`From<InvalidationReason>`] conversion) and the system-internal
/// reasons sassi constructs itself ([`EventReason::LruEvict`],
/// [`EventReason::TtlExpired`], [`EventReason::BackendInvalidation`]).
/// Subscribers see this enum on the event stream; only the
/// [`InvalidationReason`] subset is reachable through the public
/// `invalidate` call path.
///
/// The internal variants carry a `pub(crate) Internal` marker as their
/// payload: external code can pattern-match (`EventReason::LruEvict(_)`)
/// but cannot construct (`EventReason::LruEvict(...)` requires a value
/// of an unnameable type). Combined with the `#[non_exhaustive]` enum
/// marker, this means subscribers see internal reasons but never
/// synthesise them.
///
/// The variants are stable wire-level identifiers — distributed
/// backends fan invalidations across processes by reason, so adding a
/// new variant is a minor-version event and removing one is a
/// breaking change. Sassi reserves the right to add new variants in
/// future minor releases (the type is `#[non_exhaustive]`).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventReason {
    /// Caller-driven invalidation — public path
    /// [`crate::punnu::Punnu::invalidate`] with
    /// [`InvalidationReason::Manual`].
    Manual,

    /// Caller-driven invalidation — public path
    /// [`crate::punnu::Punnu::invalidate`] with
    /// [`InvalidationReason::OnSave`].
    OnSave,

    /// Caller-driven invalidation — public path
    /// [`crate::punnu::Punnu::invalidate`] with
    /// [`InvalidationReason::OnDelete`].
    OnDelete,

    /// System-internal: LRU eviction. The cache was at capacity and
    /// pushed this entry to make room for a newer one. Distinguishable
    /// from [`EventReason::TtlExpired`] for metrics: an LRU-driven
    /// eviction often indicates an undersized `lru_size`, while a
    /// TTL-driven eviction indicates the configured freshness window.
    /// Not reachable via [`crate::punnu::Punnu::invalidate`]; cannot
    /// be constructed externally (the `Internal` marker is `pub(crate)`).
    LruEvict(Internal),

    /// System-internal: the entry's TTL elapsed. Either a lazy check
    /// on `get` observed expiry, or the optional background sweep
    /// task removed the entry mid-tick. See spec §6.2.5 for the TTL
    /// contract. Not reachable via [`crate::punnu::Punnu::invalidate`];
    /// cannot be constructed externally.
    TtlExpired(Internal),

    /// System-internal: a distributed cache backend (Redis pub/sub,
    /// Postgres LISTEN/NOTIFY, …) pushed an invalidation that the
    /// [`crate::punnu::Punnu`] applied locally. Pushed by the
    /// `CacheBackend::invalidation_stream` consumer in Cluster D,
    /// Task 13; pinned now so subscribers can pattern-match against
    /// it from v0.1.0-alpha.0 onward. Not reachable via
    /// [`crate::punnu::Punnu::invalidate`]; cannot be constructed
    /// externally.
    BackendInvalidation(Internal),
}

impl From<InvalidationReason> for EventReason {
    fn from(r: InvalidationReason) -> Self {
        match r {
            InvalidationReason::Manual => Self::Manual,
            InvalidationReason::OnSave => Self::OnSave,
            InvalidationReason::OnDelete => Self::OnDelete,
        }
    }
}
