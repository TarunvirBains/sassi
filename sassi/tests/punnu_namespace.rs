//! `PunnuConfig::namespace` plumbing + validation.
//!
//! Spec §3.5: namespace governs L2 backend cache-key prefixes.
//! L1 storage is per-Punnu-instance and unaffected by namespace —
//! this file verifies the config plumbs through cleanly without
//! accidentally affecting L1 semantics, and that builder-time
//! validation rejects degenerate values (empty string).
//!
//! The L2-side namespace effect (Redis backend key prefixing) will
//! be exercised by the future redis backend integration tests; this
//! file pins the L1 contract today.

#![cfg(feature = "runtime-tokio")]

use sassi::{Cacheable, Field, Punnu, PunnuConfig};

#[derive(Debug, Clone)]
struct E {
    id: i64,
}

#[derive(Default)]
struct EFields {
    #[allow(dead_code)]
    id: Field<E, i64>,
}

impl Cacheable for E {
    type Id = i64;
    type Fields = EFields;
    fn id(&self) -> i64 {
        self.id
    }
    fn fields() -> EFields {
        EFields {
            id: Field::new("id", |e| &e.id),
        }
    }
}

#[tokio::test]
async fn namespace_does_not_affect_l1() {
    // L1 storage is per-Punnu-instance; namespace governs only L2
    // backend keys. Two Punnus with different namespaces against
    // (conceptually) the same backend never collide on L1 because
    // L1 is per-instance anyway.
    let p1 = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("env_a".into()),
            ..Default::default()
        })
        .build();
    let p2 = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("env_b".into()),
            ..Default::default()
        })
        .build();

    p1.insert(E { id: 1 }).await.unwrap();
    p2.insert(E { id: 1 }).await.unwrap();

    assert_eq!(p1.len(), 1);
    assert_eq!(p2.len(), 1);
}

#[tokio::test]
async fn namespace_value_round_trips_through_config_accessor() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("staging_v1".into()),
            ..Default::default()
        })
        .build();
    assert_eq!(p.config().namespace.as_deref(), Some("staging_v1"));
}

#[tokio::test]
async fn namespace_none_round_trips_through_config_accessor() {
    let p = Punnu::<E>::builder().build();
    assert_eq!(p.config().namespace, None);
}

#[test]
#[should_panic(expected = "PunnuConfig::namespace must be non-empty when set")]
fn empty_namespace_string_is_rejected_at_build() {
    // Degenerate config: empty-string namespace would silently
    // prefix L2 keys with a leading separator and could collide
    // with un-namespaced deployments. Builder-time guard.
    let _: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some(String::new()),
            ..Default::default()
        })
        .build();
}
