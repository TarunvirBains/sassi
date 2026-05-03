//! Eviction-recovery state for delta-refresh subscriptions.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

pub(crate) struct RecoverySet<Id> {
    entries: HashMap<Id, RecoveryEntry>,
    max_entries: usize,
    overflow_warned: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoveryEntry {
    pub(crate) attempts: u8,
    pub(crate) next_eligible_tick: u64,
}

#[must_use]
pub(crate) struct RecoverySnapshot<Id> {
    entries: Option<HashMap<Id, RecoveryEntry>>,
}

impl<Id> RecoverySet<Id>
where
    Id: Eq + Hash + Clone,
{
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
            overflow_warned: false,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.overflow_warned = false;
    }

    pub(crate) fn record_eviction(&mut self, id: Id) {
        if !self.entries.contains_key(&id) && self.entries.len() > self.max_entries {
            return;
        }
        self.entries.entry(id).or_insert(RecoveryEntry {
            attempts: 0,
            next_eligible_tick: 0,
        });
    }

    pub(crate) fn is_overflowing(&mut self) -> bool {
        let overflowing = self.entries.len() > self.max_entries;
        if overflowing && !self.overflow_warned {
            tracing::warn!(
                pending = self.entries.len(),
                max_entries = self.max_entries,
                "delta refresh recovery set overflow; forcing full refresh"
            );
            self.overflow_warned = true;
        } else if !overflowing {
            self.overflow_warned = false;
        }
        overflowing
    }

    pub(crate) fn snapshot_eligible(&mut self, current_tick: u64) -> RecoverySnapshot<Id> {
        let eligible_ids = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.next_eligible_tick <= current_tick)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let mut snapshot = HashMap::new();
        for id in eligible_ids {
            if let Some(entry) = self.entries.remove(&id) {
                snapshot.insert(id, entry);
            }
        }
        RecoverySnapshot::new(snapshot)
    }

    pub(crate) fn snapshot_all(&mut self) -> RecoverySnapshot<Id> {
        RecoverySnapshot::new(std::mem::take(&mut self.entries))
    }

    pub(crate) fn note_success(&mut self, snapshot: RecoverySnapshot<Id>) {
        let _ = snapshot.into_entries();
    }

    pub(crate) fn restore_after_failed(
        &mut self,
        snapshot: RecoverySnapshot<Id>,
        current_tick: u64,
    ) {
        for (id, mut entry) in snapshot.into_entries() {
            let delay = 1_u64 << entry.attempts.min(6);
            entry.attempts = entry.attempts.saturating_add(1);
            entry.next_eligible_tick = current_tick.saturating_add(delay);
            self.entries.entry(id).or_insert(entry);
        }
    }
}

impl<Id> RecoverySnapshot<Id>
where
    Id: Eq + Hash + Clone,
{
    pub(crate) fn empty() -> Self {
        Self::new(HashMap::new())
    }

    fn new(entries: HashMap<Id, RecoveryEntry>) -> Self {
        Self {
            entries: Some(entries),
        }
    }

    pub(crate) fn ids(&self) -> HashSet<Id> {
        self.entries
            .as_ref()
            .expect("recovery snapshot already resolved")
            .keys()
            .cloned()
            .collect()
    }

    fn into_entries(mut self) -> HashMap<Id, RecoveryEntry> {
        self.entries
            .take()
            .expect("recovery snapshot already resolved")
    }
}

impl<Id> Drop for RecoverySnapshot<Id> {
    fn drop(&mut self) {
        let unresolved = self
            .entries
            .as_ref()
            .is_some_and(|entries| !entries.is_empty());
        if !unresolved {
            return;
        }

        if cfg!(debug_assertions) && !std::thread::panicking() {
            panic!("RecoverySnapshot dropped without success or restore");
        }

        tracing::error!("RecoverySnapshot dropped without success or restore");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_restore_uses_capped_exponential_backoff() {
        let mut set = RecoverySet::new(8);
        set.record_eviction(1_i64);

        let mut delays = Vec::new();
        let mut current_tick = 100;
        for _ in 0..8 {
            let snapshot = set.snapshot_eligible(current_tick);
            let entry = snapshot
                .entries
                .as_ref()
                .and_then(|entries| entries.get(&1))
                .copied()
                .expect("id should be eligible");
            set.restore_after_failed(snapshot, current_tick);
            let restored = set.entries.get(&1).copied().unwrap();
            delays.push(restored.next_eligible_tick - current_tick);
            current_tick = restored.next_eligible_tick;
            assert_eq!(restored.attempts, entry.attempts + 1);
        }

        assert_eq!(delays, vec![1, 2, 4, 8, 16, 32, 64, 64]);
    }

    #[test]
    fn restore_preserves_newer_eviction_for_same_id() {
        let mut set = RecoverySet::new(8);
        set.record_eviction(1_i64);
        let snapshot = set.snapshot_eligible(1);
        set.record_eviction(1_i64);

        set.restore_after_failed(snapshot, 10);

        assert_eq!(
            set.entries.get(&1).copied(),
            Some(RecoveryEntry {
                attempts: 0,
                next_eligible_tick: 0
            })
        );
    }

    #[test]
    fn record_eviction_keeps_one_overflow_sentinel() {
        let mut set = RecoverySet::new(2);

        set.record_eviction(1_i64);
        set.record_eviction(2_i64);
        set.record_eviction(3_i64);
        set.record_eviction(4_i64);

        assert_eq!(set.len(), 3);
        assert!(set.is_overflowing());
    }

    #[test]
    #[should_panic(expected = "RecoverySnapshot dropped without success or restore")]
    fn unresolved_snapshot_panics_in_debug() {
        let mut set = RecoverySet::new(8);
        set.record_eviction(1_i64);
        let _snapshot = set.snapshot_eligible(1);
    }
}
