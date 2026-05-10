//! Public-surface coverage for sampled-LRU capacity pressure. The
//! unit tests in `pool.rs` exercise the true access-clock saturation
//! boundary through an in-crate seed hook; this integration test stays
//! on public APIs and verifies capacity-pressure behaviour does not
//! lose the freshly inserted value.
//!
//! The unit-level tests inside `pool.rs` cover the raw counter
//! semantics (saturation pin, monotonicity, no regression under
//! concurrent load). This file is a behavioural anchor for the public
//! insert/read/invalidate surface under LRU pressure.

#![cfg(all(feature = "serde", feature = "runtime-tokio"))]

use sassi::{Cacheable, Field, Punnu, PunnuConfig};

#[derive(Debug, Clone)]
struct Item {
    id: i64,
    label: String,
}

#[derive(Default)]
struct ItemFields {
    #[allow(dead_code)]
    id: Field<Item, i64>,
}

impl Cacheable for Item {
    type Id = i64;
    type Fields = ItemFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> ItemFields {
        ItemFields {
            id: Field::new("id", |item| &item.id),
        }
    }
}

/// Inserts continue to succeed and reads continue to return inserted
/// values under capacity pressure. Sampled-LRU may evict any older
/// sampled resident, but the fresh insert must survive the write and
/// the L1 invariants must remain coherent.
#[tokio::test]
async fn cache_remains_correct_under_lru_capacity_pressure() {
    // Use a small capacity so eviction would be exercised if it were
    // triggered incorrectly.
    let pool = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 4,
            ..Default::default()
        })
        .build();

    pool.insert(Item {
        id: 0,
        label: "warmup".into(),
    })
    .await
    .unwrap();
    pool.invalidate(&0, sassi::InvalidationReason::Manual)
        .await
        .unwrap();

    // Insert four entries — equal to `lru_size`. None should be
    // evicted because L1 is at capacity but not over.
    for id in 1..=4_i64 {
        pool.insert(Item {
            id,
            label: format!("entry-{id}"),
        })
        .await
        .unwrap();
    }
    assert_eq!(pool.len(), 4);
    for id in 1..=4_i64 {
        let item = pool
            .get(&id)
            .unwrap_or_else(|| panic!("entry {id} must be readable before capacity pressure"));
        assert_eq!(item.label, format!("entry-{id}"));
    }

    // One more insert pushes the pool over capacity and forces a
    // sampled-LRU eviction. The eviction must succeed without panic
    // and preserve the freshly inserted entry.
    pool.insert(Item {
        id: 5,
        label: "entry-5".into(),
    })
    .await
    .unwrap();
    assert_eq!(pool.len(), 4);
    assert!(
        pool.get(&5).is_some(),
        "freshly inserted id must be resident"
    );
}
