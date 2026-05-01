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
    Invalidate {
        /// Id of the entry that left.
        id: T::Id,
        /// Why the entry left.
        reason: InvalidationReason,
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

/// Why an entry left the cache. Surfaced inside
/// [`PunnuEvent::Invalidate`].
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

    /// LRU eviction — the cache was at capacity and pushed this entry
    /// to make room for a newer one. Distinguishable from
    /// [`InvalidationReason::TtlExpired`] for metrics: an LRU-driven
    /// eviction often indicates an undersized `lru_size`, while a
    /// TTL-driven eviction indicates the configured freshness window.
    LruEvict,

    /// Driven by a successful `Model::save` on the bound
    /// `DjogiContext` (the same-process invalidation path described
    /// in spec §6.1). Sassi-side semantics: the consumer surfaces this
    /// reason; sassi records it.
    OnSave,

    /// Driven by a successful `Model::delete` on the bound
    /// `DjogiContext` (spec §6.1).
    OnDelete,

    /// A distributed cache backend (Redis pub/sub, Postgres
    /// LISTEN/NOTIFY, …) pushed an invalidation that the
    /// [`crate::punnu::Punnu`] applied locally. Wires up in a later
    /// task (full backend trait); pinned now so subscribers can
    /// pattern-match against it from v0.1.0-alpha.0 onward.
    BackendInvalidation,

    /// The entry's TTL elapsed. Either a lazy check on `get`
    /// observed expiry, or the optional background sweep task removed
    /// the entry mid-tick. See spec §6.2.5 for the TTL contract.
    TtlExpired,
}
