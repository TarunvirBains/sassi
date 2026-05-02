//! Delta-apply payloads and accounting.

use crate::Cacheable;
use std::collections::HashSet;

/// Items and true-delete tombstones fetched by a delta-sync source.
///
/// Tombstones are global deletes against the `Punnu<T>` identity map,
/// not "left this query" membership signals. Absence from `items`
/// never deletes a resident entry.
#[non_exhaustive]
pub struct DeltaResult<T: Cacheable> {
    /// Items to upsert into the Punnu.
    pub items: Vec<T>,
    /// IDs known to be deleted by the source of truth.
    pub tombstones: HashSet<T::Id>,
}

impl<T: Cacheable> DeltaResult<T> {
    /// Construct a delta payload.
    pub fn new(items: Vec<T>, tombstones: HashSet<T::Id>) -> Self {
        Self { items, tombstones }
    }
}

/// Accounting returned by [`crate::punnu::Punnu::apply_delta`].
///
/// Counts describe the final committed snapshot transition. If an item
/// is accepted during delta preparation but sampled-LRU removes it
/// before publication, it is not counted as applied because readers and
/// subscribers never observe it as resident.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeltaApplyStats {
    /// Number of non-tombstoned items published as inserts or updates.
    pub applied_items: usize,
    /// Number of resident IDs removed by tombstones.
    pub tombstones_evicted: usize,
    /// Number of previously resident IDs removed by sampled-LRU.
    pub lru_evictions: usize,
}
