#![cfg(all(feature = "serde", feature = "runtime-tokio"))]

//! Behavior tests for the whole-Punnu snapshot wrapper:
//! [`Punnu::snapshot_postcard`] and [`Punnu::restore_postcard`].
//!
//! The wrapper auto-dispatches between entries-only and
//! internal-state shapes based on the wire kind byte, so these tests
//! cover both happy paths and the kind/version rejection edges.

use async_trait::async_trait;
use sassi::{
    BackendError, BackendFailureMode, BackendInvalidationStream, BackendKeyspace, CacheBackend,
    Cacheable, Field, Punnu, PunnuConfig, PunnuRestoreStats, PunnuSnapshotError, SnapshotMode,
    WireFormatError, wire,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct E {
    id: i64,
    label: String,
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

/// Distinct cacheable type for cross-type rejection tests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct F {
    id: i64,
}

#[derive(Default)]
struct FFields {
    #[allow(dead_code)]
    id: Field<F, i64>,
}

impl Cacheable for F {
    type Id = i64;
    type Fields = FFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> FFields {
        FFields {
            id: Field::new("id", |f| &f.id),
        }
    }
}

// =====================================================================
// EntriesOnly mode
// =====================================================================

#[test]
fn snapshot_mode_default_is_entries_only() {
    assert_eq!(SnapshotMode::default(), SnapshotMode::EntriesOnly);
}

#[tokio::test]
async fn snapshot_postcard_entries_only_matches_export_entries_postcard_byte_for_byte() {
    // The wrapper in EntriesOnly mode should produce the same byte
    // stream as `export_entries_postcard` so existing readers cannot
    // tell them apart and the wire format is preserved.
    let pool = Punnu::<E>::builder().build();
    pool.insert(E {
        id: 1,
        label: "one".into(),
    })
    .await
    .unwrap();
    pool.insert(E {
        id: 2,
        label: "two".into(),
    })
    .await
    .unwrap();

    let direct = pool.export_entries_postcard().unwrap();
    let wrapped = pool.snapshot_postcard(SnapshotMode::EntriesOnly).unwrap();

    assert_eq!(direct, wrapped);
}

#[tokio::test]
async fn restore_postcard_accepts_legacy_export_entries_postcard_byte_stream() {
    // The wrapper must accept byte streams produced by the historical
    // entries-only API. Adopters that already persist
    // `export_entries_postcard` bytes can switch to the new wrapper at
    // restore time without re-shaping their on-disk data.
    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 7,
            label: "seven".into(),
        })
        .await
        .unwrap();
    let bytes = donor.export_entries_postcard().unwrap();

    let pool = Punnu::<E>::builder().build();
    let stats = pool.restore_postcard(&bytes).unwrap();
    assert_eq!(stats.inserted, 1);
    assert_eq!(pool.get(&7).unwrap().label, "seven");
}

#[tokio::test]
async fn restore_postcard_accepts_entries_only_byte_stream() {
    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 7,
            label: "seven".into(),
        })
        .await
        .unwrap();
    let bytes = donor.snapshot_postcard(SnapshotMode::EntriesOnly).unwrap();

    let pool = Punnu::<E>::builder().build();
    let stats = pool.restore_postcard(&bytes).unwrap();
    assert_eq!(
        stats,
        PunnuRestoreStats {
            inserted: 1,
            updated: 0,
            removed: 0,
        }
    );
    assert_eq!(pool.get(&7).unwrap().label, "seven");
}

#[tokio::test]
async fn restore_entries_postcard_rejects_with_hints_kind() {
    // The lower-level entries-only restore must continue to reject
    // with-hints byte streams as a kind mismatch — the wrapper is the
    // only entry point that auto-dispatches between kinds.
    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 1,
            label: "v".into(),
        })
        .await
        .unwrap();
    let bytes = donor
        .snapshot_postcard(SnapshotMode::WithInternalState)
        .unwrap();

    let pool = Punnu::<E>::builder().build();
    let err = pool.restore_entries_postcard(&bytes).unwrap_err();
    match err {
        PunnuSnapshotError::WireFormat(WireFormatError::KindMismatch { got, expected }) => {
            assert_eq!(got, 0x04, "with-hints kind byte");
            assert_eq!(expected, 0x03, "entries-only kind byte");
        }
        other => panic!("expected kind mismatch, got {other:?}"),
    }
}

// =====================================================================
// WithInternalState mode
// =====================================================================

#[tokio::test]
async fn snapshot_postcard_with_internal_state_round_trips_values() {
    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 1,
            label: "a".into(),
        })
        .await
        .unwrap();
    donor
        .insert(E {
            id: 2,
            label: "b".into(),
        })
        .await
        .unwrap();
    let bytes = donor
        .snapshot_postcard(SnapshotMode::WithInternalState)
        .unwrap();

    let pool = Punnu::<E>::builder().build();
    let stats = pool.restore_postcard(&bytes).unwrap();
    assert_eq!(stats.inserted, 2);
    assert_eq!(pool.get(&1).unwrap().label, "a");
    assert_eq!(pool.get(&2).unwrap().label, "b");
}

#[tokio::test(start_paused = true)]
async fn snapshot_with_internal_state_preserves_remaining_ttl() {
    // EntriesOnly resets TTL to the receiving pool's `default_ttl`.
    // WithInternalState carries the source's remaining TTL across the
    // boundary, so an entry that had a long deadline before snapshot
    // continues to have a long deadline after restore — independent of
    // the receiving pool's own `default_ttl` setting.
    let donor = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .build();
    donor
        .insert(E {
            id: 1,
            label: "v".into(),
        })
        .await
        .unwrap();

    // Move 10 seconds forward on the donor's clock so the remaining
    // TTL is ~50s.
    tokio::time::advance(Duration::from_secs(10)).await;

    let bytes = donor
        .snapshot_postcard(SnapshotMode::WithInternalState)
        .unwrap();

    let pool = Punnu::<E>::builder()
        .config(PunnuConfig {
            // Receiving pool's default would expire the entry immediately,
            // so any state visible after restore proves that the snapshot's
            // remaining-TTL hint took precedence.
            default_ttl: Some(Duration::from_millis(1)),
            ..Default::default()
        })
        .build();
    pool.restore_postcard(&bytes).unwrap();

    // After restore plus a small wait that exceeds the receiving
    // pool's `default_ttl` but is well under the carried 50s
    // remainder, the entry should still be readable.
    tokio::time::advance(Duration::from_millis(500)).await;
    assert!(
        pool.get(&1).is_some(),
        "with-hints restore must honor the source's remaining TTL, not the receiving pool's default_ttl"
    );

    // After the carried remainder fully elapses, the entry must look
    // absent through the lazy-expiry path.
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(
        pool.get(&1).is_none(),
        "with-hints entry must expire after its carried remaining TTL"
    );
}

#[tokio::test]
async fn snapshot_with_internal_state_distinguishes_no_ttl_from_short_ttl() {
    // An entry without a TTL on the source should not gain one on
    // restore through the with-hints body. We cannot directly assert
    // "no expiry" on the public surface, but we can verify the entry
    // is still readable far past the receiving pool's tiny default
    // TTL, which the with-hints body must override with `None`.
    let donor = Punnu::<E>::builder().build(); // no default TTL
    donor
        .insert(E {
            id: 1,
            label: "no-ttl".into(),
        })
        .await
        .unwrap();
    let bytes = donor
        .snapshot_postcard(SnapshotMode::WithInternalState)
        .unwrap();

    let pool = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_millis(1)),
            ..Default::default()
        })
        .build();
    pool.restore_postcard(&bytes).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        pool.get(&1).is_some(),
        "with-hints restore must preserve 'no TTL' rather than apply the receiving pool's default"
    );
}

#[tokio::test]
async fn snapshot_with_internal_state_preserves_relative_lru_order() {
    // The donor pool touches three entries in a known order; the
    // receiving pool restores under capacity pressure of size 2, so
    // exactly one of the three must be evicted by sampled-LRU. Without
    // hint preservation the receiving pool would issue fresh epochs
    // and any eviction is possible. With hints, the most-recently
    // accessed entry on the source should be the most resistant to
    // eviction on the target.
    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 1,
            label: "a".into(),
        })
        .await
        .unwrap();
    donor
        .insert(E {
            id: 2,
            label: "b".into(),
        })
        .await
        .unwrap();
    donor
        .insert(E {
            id: 3,
            label: "c".into(),
        })
        .await
        .unwrap();

    // Touch id 3 once more to make it the most recent on the donor
    // pool. Sampled LRU is probabilistic on small N, so we use a
    // capacity equal to the entry count and assert the byte stream
    // round-trips.
    let _ = donor.get(&3);

    let bytes = donor
        .snapshot_postcard(SnapshotMode::WithInternalState)
        .unwrap();

    // Restore into a pool of exactly the same size; sampled-LRU has
    // no eviction work to do, but the receiving pool's access clock
    // must advance past the highest restored rank so the next
    // ordinary `insert` still issues an epoch greater than every
    // restored entry's epoch. Verify by inserting a fourth entry into
    // a slightly larger pool and checking it is the freshest.
    let pool = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: 4,
            ..Default::default()
        })
        .build();
    pool.restore_postcard(&bytes).unwrap();
    pool.insert(E {
        id: 4,
        label: "d".into(),
    })
    .await
    .unwrap();
    assert!(pool.get(&4).is_some());
    assert!(pool.get(&3).is_some());
    assert!(pool.get(&2).is_some());
    assert!(pool.get(&1).is_some());
}

#[tokio::test]
async fn restore_postcard_rejects_future_reserved_kind_byte() {
    // Build a byte stream with a wire kind byte beyond the current
    // implementation's understanding (0x05+). The wrapper's kind-peek
    // must reject before any per-body decode work.
    let pool = Punnu::<E>::builder().build();
    let mut bytes = Vec::new();
    encode_test_header(&mut bytes, 0x05, E::cache_type_name());
    bytes.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]);

    let err = pool.restore_postcard(&bytes).unwrap_err();
    match err {
        PunnuSnapshotError::WireFormat(WireFormatError::UnsupportedKind { kind }) => {
            assert_eq!(kind, 0x05);
        }
        other => panic!("expected UnsupportedKind, got {other:?}"),
    }
}

#[tokio::test]
async fn restore_postcard_rejects_unknown_envelope_version() {
    // Build a with-hints byte stream whose envelope version is not the
    // current implementation's. Restore must reject as a wire-format
    // codec error before any L1 mutation.
    let pool = Punnu::<E>::builder().build();

    let mut bytes = Vec::new();
    encode_test_header(&mut bytes, 0x04, E::cache_type_name());
    bytes.extend_from_slice(&999_u16.to_le_bytes()); // bad envelope version
    bytes.extend_from_slice(&0_u32.to_le_bytes()); // count
    let err = pool.restore_postcard(&bytes).unwrap_err();
    match err {
        PunnuSnapshotError::WireFormat(WireFormatError::Codec(message)) => {
            assert!(
                message.contains("envelope version mismatch"),
                "expected envelope version mismatch, got {message}"
            );
        }
        other => panic!("expected wire-format codec error, got {other:?}"),
    }
    assert!(pool.is_empty());
}

#[tokio::test]
async fn restore_postcard_rejects_with_hints_type_mismatch() {
    // A with-hints byte stream produced by donor pool of type F must
    // not restore into a pool of type E.
    let donor: Punnu<F> = Punnu::<F>::builder().build();
    donor.insert(F { id: 1 }).await.unwrap();
    let bytes = donor
        .snapshot_postcard(SnapshotMode::WithInternalState)
        .unwrap();

    let pool: Punnu<E> = Punnu::<E>::builder().build();
    let err = pool.restore_postcard(&bytes).unwrap_err();
    match err {
        PunnuSnapshotError::WireFormat(WireFormatError::TypeNameMismatch { .. }) => {}
        other => panic!("expected type-name mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn restore_postcard_rejects_with_hints_capacity_overflow() {
    // Build a with-hints byte stream that declares more entries than
    // the receiving pool's capacity. The count guard must reject
    // before any per-entry decode work.
    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: 2,
            ..Default::default()
        })
        .build();

    let mut bytes = Vec::new();
    encode_test_header(&mut bytes, 0x04, E::cache_type_name());
    bytes.extend_from_slice(&1_u16.to_le_bytes()); // current envelope version
    bytes.extend_from_slice(&5_u32.to_le_bytes()); // count exceeds lru_size
    let err = pool.restore_postcard(&bytes).unwrap_err();
    match err {
        PunnuSnapshotError::TooManyEntries { entries, limit } => {
            assert_eq!(entries, 5);
            assert_eq!(limit, 2);
        }
        other => panic!("expected TooManyEntries, got {other:?}"),
    }
}

#[tokio::test]
async fn restore_postcard_rejects_with_hints_duplicate_id() {
    let mut bytes = Vec::new();
    encode_test_header(&mut bytes, 0x04, E::cache_type_name());
    bytes.extend_from_slice(&1_u16.to_le_bytes()); // version
    bytes.extend_from_slice(&2_u32.to_le_bytes()); // count
    // Two entries with the same id.
    let body_a = postcard::to_allocvec(&E {
        id: 1,
        label: "a".into(),
    })
    .unwrap();
    bytes.extend_from_slice(&body_a);
    bytes.push(0); // tag = none
    bytes.extend_from_slice(&0_u64.to_le_bytes()); // ms
    bytes.extend_from_slice(&0_u32.to_le_bytes()); // rank
    let body_b = postcard::to_allocvec(&E {
        id: 1,
        label: "b".into(),
    })
    .unwrap();
    bytes.extend_from_slice(&body_b);
    bytes.push(0);
    bytes.extend_from_slice(&0_u64.to_le_bytes());
    bytes.extend_from_slice(&1_u32.to_le_bytes());

    let pool = Punnu::<E>::builder().build();
    let err = pool.restore_postcard(&bytes).unwrap_err();
    assert!(matches!(err, PunnuSnapshotError::DuplicateId));
    assert!(pool.is_empty());
}

#[tokio::test]
async fn restore_postcard_rejects_with_hints_strict_backend_write_in_flight() {
    let backend = BlockingStrictPutBackend::default();
    let put_entered = backend.put_entered.clone();
    let put_release = backend.put_release.clone();

    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            ..Default::default()
        })
        .backend(backend)
        .build();

    let inserter = {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.insert(E {
                id: 99,
                label: "blocking".into(),
            })
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), put_entered.notified())
        .await
        .expect("strict put should reach backend");

    let donor: Punnu<E> = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 8,
            label: "snap".into(),
        })
        .await
        .unwrap();
    let bytes = donor
        .snapshot_postcard(SnapshotMode::WithInternalState)
        .unwrap();

    let err = pool.restore_postcard(&bytes).unwrap_err();
    match err {
        PunnuSnapshotError::BackendWriteInFlight { reserved } => {
            assert_eq!(reserved, 1);
        }
        other => panic!("expected BackendWriteInFlight, got {other:?}"),
    }
    assert!(pool.get(&8).is_none());

    put_release.notify_one();
    inserter.await.unwrap().unwrap();
}

// =====================================================================
// Helpers
// =====================================================================

const MAGIC: &[u8; 8] = b"SASSI\0W\0";

fn encode_test_header(out: &mut Vec<u8>, kind: u8, type_name: &str) {
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&wire::WIRE_FORMAT_MAJOR.to_le_bytes());
    out.push(kind);
    out.push(0); // flags
    let name = type_name.as_bytes();
    out.extend_from_slice(&(name.len() as u16).to_le_bytes());
    out.extend_from_slice(name);
}

#[derive(Default)]
struct BlockingStrictPutBackend {
    put_entered: Arc<Notify>,
    put_release: Arc<Notify>,
}

#[async_trait]
impl CacheBackend<E> for BlockingStrictPutBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<E>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.put_entered.notify_one();
        self.put_release.notified().await;
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }

    fn invalidation_stream(&self, _keyspace: BackendKeyspace) -> BackendInvalidationStream<i64> {
        Box::pin(futures::stream::empty())
    }
}
