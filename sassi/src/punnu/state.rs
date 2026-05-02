//! Snapshot L1 state substrate for [`crate::punnu::Punnu`].
//!
//! This module is intentionally parallel to the current `RwLock<LruCache>`
//! storage in `pool.rs`. Task 12B will switch `Punnu<T>` over to this
//! substrate; for now the module owns the data invariants and tests.

// Task 12A lands the substrate before Task 12B starts consuming it.
#![allow(dead_code)]

use crate::cacheable::Cacheable;
use crate::time::Instant;
use im::{HashMap, Vector};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// L1 storage cell holding a cached value and access metadata.
pub(crate) struct Entry<T> {
    /// Shared handle to the cached payload.
    pub(crate) value: Arc<T>,

    /// Absolute expiry deadline. `None` means no TTL expiry.
    pub(crate) expires_at: Option<Instant>,

    /// Monotonic access marker used by sampled-LRU eviction.
    last_access_epoch: AtomicU64,
}

impl<T> Entry<T> {
    /// Construct a new L1 entry.
    pub(crate) fn new(value: Arc<T>, expires_at: Option<Instant>, epoch: u64) -> Self {
        Self {
            value,
            expires_at,
            last_access_epoch: AtomicU64::new(epoch),
        }
    }

    /// Return whether the entry is expired at `now`.
    pub(crate) fn is_expired_at(&self, now: Instant) -> bool {
        match self.expires_at {
            Some(deadline) => deadline <= now,
            None => false,
        }
    }

    /// Set the sampled-LRU access epoch.
    pub(crate) fn bump_access_epoch(&self, epoch: u64) {
        self.last_access_epoch.store(epoch, Ordering::Relaxed);
    }

    /// Read the sampled-LRU access epoch.
    pub(crate) fn access_epoch(&self) -> u64 {
        self.last_access_epoch.load(Ordering::Relaxed)
    }
}

/// Immutable-snapshot-friendly L1 state.
///
/// `entries` stores payloads by id, `keys` supports random-access sampling
/// over the vector, and `positions` maps ids back into `keys` for coherent
/// swap-remove.
pub(crate) struct L1State<T: Cacheable> {
    pub(crate) entries: HashMap<T::Id, Arc<Entry<T>>>,
    pub(crate) keys: Vector<T::Id>,
    pub(crate) positions: HashMap<T::Id, usize>,
}

impl<T: Cacheable> Clone for L1State<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            keys: self.keys.clone(),
            positions: self.positions.clone(),
        }
    }
}

impl<T: Cacheable> L1State<T> {
    /// Construct an empty L1 state snapshot.
    pub(crate) fn empty() -> Self {
        Self {
            entries: HashMap::new(),
            keys: Vector::new(),
            positions: HashMap::new(),
        }
    }

    /// Number of cached entries.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the state is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return the entry for `id`, if present.
    pub(crate) fn get(&self, id: &T::Id) -> Option<&Arc<Entry<T>>> {
        self.entries.get(id)
    }

    /// Return whether `id` is present.
    pub(crate) fn contains_key(&self, id: &T::Id) -> bool {
        self.entries.contains_key(id)
    }

    /// Insert `entry` at `id`, returning the replaced entry if any.
    pub(crate) fn insert_entry(
        &mut self,
        id: T::Id,
        entry: Arc<Entry<T>>,
    ) -> Option<Arc<Entry<T>>> {
        assert!(
            id == entry.value.id(),
            "L1State invariant violated: insert key does not match entry payload id"
        );

        if !self.entries.contains_key(&id) {
            self.positions.insert(id.clone(), self.keys.len());
            self.keys.push_back(id.clone());
        }

        self.entries.insert(id, entry)
    }

    /// Remove `id` with swap-remove semantics over `keys`.
    pub(crate) fn remove_entry(&mut self, id: &T::Id) -> Option<Arc<Entry<T>>> {
        let removed = self.entries.remove(id)?;
        let position = self
            .positions
            .remove(id)
            .expect("L1State invariant violated: entry missing position");
        let last_index = self.keys.len() - 1;

        if position == last_index {
            let _ = self.keys.pop_back();
            return Some(removed);
        }

        let moved_id = self
            .keys
            .get(last_index)
            .expect("L1State invariant violated: last key missing")
            .clone();
        let removed_id = self.keys.set(position, moved_id.clone());
        debug_assert!(removed_id == *id);
        let _ = self.keys.pop_back();
        self.positions.insert(moved_id, position);

        Some(removed)
    }

    /// Assert that `entries`, `keys`, and `positions` describe the same set.
    pub(crate) fn assert_invariants(&self) {
        assert!(
            self.entries.len() == self.keys.len(),
            "L1State invariant violated: entries and keys lengths differ"
        );
        assert!(
            self.entries.len() == self.positions.len(),
            "L1State invariant violated: entries and positions lengths differ"
        );

        for (index, id) in self.keys.iter().enumerate() {
            assert!(
                self.entries.contains_key(id),
                "L1State invariant violated: key has no entry"
            );
            let Some(position) = self.positions.get(id) else {
                panic!("L1State invariant violated: key has no position");
            };
            assert!(
                *position == index,
                "L1State invariant violated: position maps to wrong index"
            );
            let Some(indexed_id) = self.keys.get(*position) else {
                panic!("L1State invariant violated: position outside key vector");
            };
            assert!(
                indexed_id == id,
                "L1State invariant violated: position maps to wrong id"
            );
        }

        for (id, _entry) in self.entries.iter() {
            assert!(
                self.positions.contains_key(id),
                "L1State invariant violated: entry has no position"
            );
        }

        for (id, entry) in self.entries.iter() {
            let payload_id = entry.value.id();
            assert!(
                id == &payload_id,
                "L1State invariant violated: map key does not match entry payload id"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cacheable::Cacheable;
    use crate::punnu::eviction::choose_sampled_lru_victim;
    use proptest::prelude::*;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    #[derive(Debug)]
    struct Item {
        id: i64,
    }

    #[derive(Default)]
    struct ItemFields;

    impl Cacheable for Item {
        type Id = i64;
        type Fields = ItemFields;

        fn id(&self) -> Self::Id {
            self.id
        }

        fn fields() -> Self::Fields {
            ItemFields
        }
    }

    fn entry(id: i64) -> Arc<Entry<Item>> {
        Arc::new(Entry::new(Arc::new(Item { id }), None, 0))
    }

    #[test]
    fn entry_new_should_initialize_access_epoch() {
        let entry = Entry::new(Arc::new(Item { id: 7 }), None, 42);

        assert_eq!(entry.access_epoch(), 42);
    }

    #[test]
    #[should_panic(
        expected = "L1State invariant violated: insert key does not match entry payload id"
    )]
    fn insert_entry_should_reject_mismatched_entry_identity() {
        let mut state = L1State::empty();

        state.insert_entry(1, entry(2));
    }

    #[test]
    fn insert_entry_should_replace_without_duplicating_keys() {
        let mut state = L1State::empty();
        let original = entry(7);
        let replacement = entry(7);

        assert!(state.insert_entry(7, original).is_none());
        let removed = state.insert_entry(7, replacement.clone());

        state.assert_invariants();
        assert!(removed.is_some());
        assert_eq!(state.len(), 1);
        assert_eq!(state.keys.iter().cloned().collect::<Vec<_>>(), vec![7]);
        assert!(Arc::ptr_eq(
            state.get(&7).expect("replacement entry should be present"),
            &replacement
        ));
    }

    #[test]
    fn remove_entry_should_swap_remove_and_update_moved_key_position() {
        let mut state = L1State::empty();
        state.insert_entry(1, entry(1));
        let removed = entry(2);
        state.insert_entry(2, removed.clone());
        state.insert_entry(3, entry(3));

        let actual = state.remove_entry(&2).expect("entry should be removed");

        state.assert_invariants();
        assert!(Arc::ptr_eq(&actual, &removed));
        assert_eq!(state.keys.iter().cloned().collect::<Vec<_>>(), vec![1, 3]);
        assert_eq!(state.positions.get(&3), Some(&1));
        assert!(!state.positions.contains_key(&2));
    }

    #[test]
    fn choose_sampled_lru_victim_should_return_none_for_empty_state() {
        let state = L1State::<Item>::empty();
        let mut rng = fastrand::Rng::with_seed(1);

        assert_eq!(choose_sampled_lru_victim(&state, &mut rng), None);
    }

    #[test]
    fn choose_sampled_lru_victim_should_choose_lowest_epoch_when_sample_covers_state() {
        let mut state = L1State::empty();
        let newest = entry(1);
        newest.bump_access_epoch(30);
        let oldest = entry(2);
        oldest.bump_access_epoch(5);
        let middle = entry(3);
        middle.bump_access_epoch(20);

        state.insert_entry(1, newest);
        state.insert_entry(2, oldest);
        state.insert_entry(3, middle);

        let mut rng = fastrand::Rng::with_seed(1);

        assert_eq!(choose_sampled_lru_victim(&state, &mut rng), Some(2));
    }

    proptest! {
        #[test]
        fn insert_remove_sequences_should_keep_state_invariants(
            operations in prop::collection::vec((any::<bool>(), -12_i64..=12), 0..200)
        ) {
            let mut state = L1State::empty();
            let mut expected_ids = BTreeSet::new();

            for (insert, id) in operations {
                if insert {
                    state.insert_entry(id, entry(id));
                    expected_ids.insert(id);
                } else {
                    state.remove_entry(&id);
                    expected_ids.remove(&id);
                }

                state.assert_invariants();
                prop_assert_eq!(state.len(), expected_ids.len());
                for expected_id in &expected_ids {
                    prop_assert!(state.contains_key(expected_id));
                }
            }
        }
    }
}
