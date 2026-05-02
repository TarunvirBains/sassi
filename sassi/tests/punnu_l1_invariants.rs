//! Public L1 invariants under snapshot-swap writes and sampled-LRU pressure.

use sassi::{Cacheable, Field, Punnu, PunnuConfig};
use std::time::Duration;

#[derive(Debug, Clone)]
struct Item {
    id: i64,
    version: i64,
}

#[derive(Default)]
struct ItemFields {
    #[allow(dead_code)]
    id: Field<Item, i64>,
    #[allow(dead_code)]
    version: Field<Item, i64>,
}

impl Cacheable for Item {
    type Id = i64;
    type Fields = ItemFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> Self::Fields {
        ItemFields {
            id: Field::new("id", |item| &item.id),
            version: Field::new("version", |item| &item.version),
        }
    }
}

#[tokio::test]
async fn sampled_lru_never_exceeds_capacity_under_insert_pressure() {
    const CAPACITY: usize = 8;
    const INSERTS: i64 = 256;

    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: CAPACITY,
            ..Default::default()
        })
        .build();

    for id in 0..INSERTS {
        p.insert(Item { id, version: id }).await.unwrap();
        assert!(
            p.len() <= CAPACITY,
            "L1 len {} exceeded configured capacity {CAPACITY} after inserting id {id}",
            p.len()
        );
    }

    let mut resident = 0;
    for id in 0..INSERTS {
        if let Some(item) = p.get(&id) {
            resident += 1;
            assert_eq!(item.id, id, "get returned a value for the wrong id");
            assert_eq!(item.version, id, "get returned a stale value for id {id}");
        }
    }

    assert_eq!(
        resident,
        p.len(),
        "public get view should account for exactly the resident L1 entries"
    );
    assert!(resident <= CAPACITY);
}

#[tokio::test(start_paused = true)]
async fn capacity_pressure_reclaims_expired_entries_before_sampled_lru() {
    let p = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 3,
            ..Default::default()
        })
        .build();

    p.insert_with_ttl(Item { id: 1, version: 10 }, Duration::from_secs(5))
        .await
        .unwrap();
    p.insert_with_ttl(Item { id: 2, version: 20 }, Duration::from_secs(60))
        .await
        .unwrap();
    p.insert_with_ttl(Item { id: 3, version: 30 }, Duration::from_secs(60))
        .await
        .unwrap();

    tokio::time::advance(Duration::from_secs(10)).await;
    p.insert_with_ttl(Item { id: 4, version: 40 }, Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(p.len(), 3);
    assert!(p.get(&1).is_none(), "expired id should be reclaimed first");

    for (id, version) in [(2, 20), (3, 30), (4, 40)] {
        let item = p
            .get(&id)
            .unwrap_or_else(|| panic!("fresh id {id} should remain resident"));
        assert_eq!(item.id, id, "get returned a value for the wrong id");
        assert_eq!(item.version, version, "get returned a stale value");
    }
}
