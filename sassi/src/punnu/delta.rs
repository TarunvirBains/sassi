//! Delta-apply payloads and accounting.

use crate::Cacheable;
use crate::watermark::DeltaSyncCacheable;
use std::collections::HashSet;

/// Items and true-delete tombstones fetched by a delta-sync source.
///
/// Tombstones are global deletes against the `Punnu<T>` identity map,
/// not "left this query" membership signals. Absence from `items`
/// never deletes a resident entry.
#[non_exhaustive]
pub struct DeltaResult<T: Cacheable, W = ()> {
    /// Items to upsert into the Punnu.
    pub items: Vec<T>,
    /// IDs known to be deleted by the source of truth.
    pub tombstones: HashSet<T::Id>,
    /// Optional source-observed high watermark for this delta result.
    ///
    /// Delta refresh subscriptions use this when a batch contains only
    /// tombstones, when the upstream source can report progress past
    /// rows not represented as cacheable items, or when cache retention
    /// should not pin source progress. If this is `None`, subscriptions
    /// infer progress from the maximum watermark in `items`.
    pub high_watermark: Option<W>,
}

impl<T: Cacheable, W> DeltaResult<T, W> {
    /// Construct a delta payload.
    pub fn new(items: Vec<T>, tombstones: HashSet<T::Id>) -> Self {
        Self {
            items,
            tombstones,
            high_watermark: None,
        }
    }

    /// Construct a delta payload with an explicit source-observed high watermark.
    pub fn with_high_watermark(
        items: Vec<T>,
        tombstones: HashSet<T::Id>,
        high_watermark: W,
    ) -> Self {
        Self {
            items,
            tombstones,
            high_watermark: Some(high_watermark),
        }
    }

    pub(crate) fn without_high_watermark(self) -> DeltaResult<T> {
        DeltaResult {
            items: self.items,
            tombstones: self.tombstones,
            high_watermark: None,
        }
    }
}

impl<T: DeltaSyncCacheable> DeltaResult<T, T::Watermark> {
    pub(crate) fn observed_watermark(&self) -> Option<T::Watermark> {
        let item_watermark = self.items.iter().map(DeltaSyncCacheable::watermark).max();
        match (&self.high_watermark, item_watermark) {
            (Some(high_watermark), Some(item_watermark)) => {
                Some(high_watermark.clone().max(item_watermark))
            }
            (Some(high_watermark), None) => Some(high_watermark.clone()),
            (None, Some(item_watermark)) => Some(item_watermark),
            (None, None) => None,
        }
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
