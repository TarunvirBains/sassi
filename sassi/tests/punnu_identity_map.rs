//! `Punnu<T>` identity-map basics: insert, get, invalidate,
//! LRU eviction.
//!
//! Each test hand-writes the [`sassi::Cacheable`] impl rather than
//! reaching for the derive macro — predicate filtering is not the
//! focus of this cluster, and the hand-impl path needs to keep
//! working anyway (spec §3.1 docs the contract for adopters who
//! can't or don't want to use the derive).

use sassi::{Cacheable, Field, InvalidationReason, OnConflict, Punnu, PunnuConfig};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Item {
    id: i64,
    name: String,
}

#[derive(Default)]
struct ItemFields {
    #[allow(dead_code)]
    id: Field<Item, i64>,
    #[allow(dead_code)]
    name: Field<Item, String>,
}

impl Cacheable for Item {
    type Id = i64;
    type Fields = ItemFields;
    fn id(&self) -> i64 {
        self.id
    }
    fn fields() -> ItemFields {
        ItemFields {
            id: Field::new("id", |i| &i.id),
            name: Field::new("name", |i| &i.name),
        }
    }
}

#[tokio::test]
async fn insert_and_get_round_trip() {
    let punnu = Punnu::<Item>::builder().build();
    let inserted: Arc<Item> = punnu
        .insert(Item {
            id: 1,
            name: "a".into(),
        })
        .await
        .unwrap();
    assert_eq!(inserted.id, 1);
    assert_eq!(inserted.name, "a");

    let fetched = punnu.get(&1).expect("entry should be cached");
    assert_eq!(fetched.id, 1);
    assert_eq!(fetched.name, "a");
    assert_eq!(punnu.len(), 1);
    assert!(!punnu.is_empty());
}

#[tokio::test]
async fn last_write_wins_replaces_entry() {
    let punnu = Punnu::<Item>::builder().build();
    punnu
        .insert(Item {
            id: 1,
            name: "v1".into(),
        })
        .await
        .unwrap();
    punnu
        .insert(Item {
            id: 1,
            name: "v2".into(),
        })
        .await
        .unwrap();

    assert_eq!(punnu.get(&1).unwrap().name, "v2");
    // Identity-map invariant: replace doesn't grow the map.
    assert_eq!(punnu.len(), 1);
}

#[tokio::test]
async fn invalidate_drops_entry() {
    let punnu = Punnu::<Item>::builder().build();
    punnu
        .insert(Item {
            id: 1,
            name: "x".into(),
        })
        .await
        .unwrap();
    assert!(punnu.get(&1).is_some());

    punnu.invalidate(&1, InvalidationReason::Manual).await;
    assert!(punnu.get(&1).is_none());
    assert_eq!(punnu.len(), 0);
    assert!(punnu.is_empty());
}

#[tokio::test]
async fn invalidate_unknown_id_is_noop() {
    // Idempotency: invalidating a missing id must not panic, must
    // not error, and must not produce an event (the test for the
    // "no event" half lives in `punnu_events.rs`; the "no panic"
    // half is here).
    let punnu = Punnu::<Item>::builder().build();
    punnu.invalidate(&999, InvalidationReason::Manual).await;
    assert_eq!(punnu.len(), 0);
}

#[tokio::test]
async fn lru_eviction_at_capacity() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 2,
            ..Default::default()
        })
        .build();
    punnu
        .insert(Item {
            id: 1,
            name: "a".into(),
        })
        .await
        .unwrap();
    punnu
        .insert(Item {
            id: 2,
            name: "b".into(),
        })
        .await
        .unwrap();
    punnu
        .insert(Item {
            id: 3,
            name: "c".into(),
        })
        .await
        .unwrap();

    // With only three residents, sampled-LRU covers the whole state
    // and selects the lowest access epoch.
    assert!(
        punnu.get(&1).is_none(),
        "id 1 should have been sampled-LRU evicted"
    );
    assert!(punnu.get(&2).is_some());
    assert!(punnu.get(&3).is_some());
    assert_eq!(punnu.len(), 2);
}

#[tokio::test]
async fn lru_get_refreshes_recency() {
    // After touching id 1 via `get`, the next insert at capacity
    // should evict id 2 (now LRU) rather than id 1.
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 2,
            ..Default::default()
        })
        .build();
    punnu
        .insert(Item {
            id: 1,
            name: "a".into(),
        })
        .await
        .unwrap();
    punnu
        .insert(Item {
            id: 2,
            name: "b".into(),
        })
        .await
        .unwrap();

    let _ = punnu.get(&1); // refresh recency

    punnu
        .insert(Item {
            id: 3,
            name: "c".into(),
        })
        .await
        .unwrap();

    assert!(punnu.get(&1).is_some(), "id 1 was just touched");
    assert!(
        punnu.get(&2).is_none(),
        "id 2 should now be the LRU candidate"
    );
    assert!(punnu.get(&3).is_some());
}

#[tokio::test]
async fn on_conflict_reject_returns_error_and_keeps_existing() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Reject,
            ..Default::default()
        })
        .build();
    punnu
        .insert(Item {
            id: 1,
            name: "first".into(),
        })
        .await
        .unwrap();

    let err = punnu
        .insert(Item {
            id: 1,
            name: "second".into(),
        })
        .await
        .expect_err("Reject must surface a Conflict on duplicate id");
    assert!(matches!(err, sassi::InsertError::Conflict));

    assert_eq!(
        punnu.get(&1).unwrap().name,
        "first",
        "rejected insert must not overwrite"
    );
}

#[test]
fn clones_share_state() {
    // `Punnu<T>` is `Clone` and observes the same identity map across
    // clones — this is the intended sharing pattern.
    let punnu_a = Punnu::<Item>::builder().build();
    let punnu_b = punnu_a.clone();

    // Use the runtime in a contained block so the `#[test]` (sync)
    // doesn't fight `#[tokio::test]`.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        punnu_a
            .insert(Item {
                id: 7,
                name: "z".into(),
            })
            .await
            .unwrap();
    });

    assert!(punnu_b.get(&7).is_some(), "clone must observe the insert");
    assert_eq!(punnu_b.len(), 1);
}

#[test]
#[should_panic(expected = "PunnuConfig::lru_size must be non-zero")]
fn build_panics_on_zero_lru_size() {
    let _ = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 0,
            ..Default::default()
        })
        .build();
}

#[test]
#[should_panic(expected = "PunnuConfig::ttl_sweep_interval must be greater than Duration::ZERO")]
fn build_panics_on_zero_ttl_sweep_interval() {
    // Guard against a `tokio::time::interval(Duration::ZERO)` panic
    // at sweep-spawn time. The builder catches the bad shape with a
    // descriptive message; consumers who want "no sweep" must pass
    // `None`.
    let _ = Punnu::<Item>::builder()
        .config(PunnuConfig {
            ttl_sweep_interval: Some(std::time::Duration::ZERO),
            ..Default::default()
        })
        .build();
}

#[test]
#[cfg(not(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
)))]
#[should_panic(
    expected = "PunnuConfig::ttl_sweep_interval requires `runtime-tokio` on native targets or `runtime-wasm` on wasm32"
)]
fn build_panics_on_ttl_sweep_without_target_compatible_runtime() {
    let _ = Punnu::<Item>::builder()
        .config(PunnuConfig {
            ttl_sweep_interval: Some(std::time::Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
}

#[test]
#[should_panic(expected = "PunnuConfig::event_channel_capacity must be greater than 0")]
fn build_panics_on_zero_event_channel_capacity() {
    // Guard against a `tokio::sync::broadcast::channel(0)` panic at
    // build time. The builder catches the bad shape and surfaces a
    // descriptive message instead of the cryptic tokio panic.
    let _ = Punnu::<Item>::builder()
        .config(PunnuConfig {
            event_channel_capacity: 0,
            ..Default::default()
        })
        .build();
}
