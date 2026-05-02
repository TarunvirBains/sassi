//! Sampled eviction helpers for snapshot L1 state.

// Task 12A lands the substrate before Task 12B starts consuming it.
#![allow(dead_code)]

use crate::cacheable::Cacheable;
use crate::punnu::state::L1State;

pub(crate) const SAMPLE_SIZE: usize = 5;

/// Choose the lowest-access-epoch id from a random sample.
pub(crate) fn choose_sampled_lru_victim<T: Cacheable>(
    state: &L1State<T>,
    rng: &mut fastrand::Rng,
) -> Option<T::Id> {
    if state.is_empty() {
        return None;
    }

    let sample_size = state.len().min(SAMPLE_SIZE);
    let mut indices = Vec::with_capacity(sample_size);
    while indices.len() < sample_size {
        let index = rng.usize(..state.len());
        if !indices.contains(&index) {
            indices.push(index);
        }
    }

    let mut victim = None;
    let mut victim_epoch = u64::MAX;

    for index in indices {
        let id = state
            .keys
            .get(index)
            .expect("L1State invariant violated: sampled index missing");
        let entry = state
            .get(id)
            .expect("L1State invariant violated: sampled key missing entry");
        let epoch = entry.access_epoch();

        if victim.is_none() || epoch < victim_epoch {
            victim = Some(id.clone());
            victim_epoch = epoch;
        }
    }

    victim
}
