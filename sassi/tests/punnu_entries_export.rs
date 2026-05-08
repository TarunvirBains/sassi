#![cfg(all(feature = "serde", feature = "runtime-tokio"))]

//! Behavior tests for `Punnu::export_entries_postcard` /
//! `Punnu::restore_entries_postcard`.
//!
//! These tests stay project-agnostic and exercise the public surface
//! that adopters use: typed identity contracts, the L1 substrate, the
//! postcard-backed entries snapshot, and the strict-backend write
//! reservation contract.

use async_trait::async_trait;
use futures::stream;
use sassi::{
    BackendError, BackendFailureMode, BackendInvalidationStream, BackendKeyspace, CacheBackend,
    Cacheable, EventReason, Field, OnConflict, Punnu, PunnuConfig, PunnuEvent, PunnuRestoreStats,
    PunnuSnapshotError, WireFormatError, wire,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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

/// Distinct cacheable type used to provoke type-name mismatches.
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

/// Cacheable type that intentionally does not implement `Clone`.
/// Used to assert that exporting from `Punnu<T>` does not require
/// `T: Clone` — it should clone `Arc<T>` handles, not payloads.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NoCloneE {
    id: i64,
    label: String,
}

#[derive(Default)]
struct NoCloneEFields {
    #[allow(dead_code)]
    id: Field<NoCloneE, i64>,
}

impl Cacheable for NoCloneE {
    type Id = i64;
    type Fields = NoCloneEFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> NoCloneEFields {
        NoCloneEFields {
            id: Field::new("id", |e| &e.id),
        }
    }
}

#[tokio::test]
async fn export_entries_postcard_round_trips_unexpired_entries() {
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

    let bytes = pool.export_entries_postcard().unwrap();

    let restored = Punnu::<E>::builder().build();
    let stats = restored.restore_entries_postcard(&bytes).unwrap();

    assert_eq!(
        stats,
        PunnuRestoreStats {
            inserted: 2,
            updated: 0,
            removed: 0,
        }
    );
    assert_eq!(restored.get(&1).unwrap().label, "one");
    assert_eq!(restored.get(&2).unwrap().label, "two");
    assert_eq!(restored.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn export_entries_postcard_skips_expired_entries() {
    let pool = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
    pool.insert(E {
        id: 1,
        label: "alive".into(),
    })
    .await
    .unwrap();
    pool.insert_with_ttl(
        E {
            id: 2,
            label: "expires".into(),
        },
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    tokio::time::advance(Duration::from_secs(2)).await;

    let bytes = pool.export_entries_postcard().unwrap();
    let restored = Punnu::<E>::builder().build();
    let stats = restored.restore_entries_postcard(&bytes).unwrap();

    // Both entries' TTLs have elapsed: nothing to export.
    assert_eq!(stats.inserted, 0);
    assert_eq!(restored.len(), 0);
}

#[test]
fn export_entries_postcard_is_deterministic_by_id_order() {
    let pool = Punnu::<E>::builder().build();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        // Insert in shuffled order; export must serialize sorted by id.
        for id in [42_i64, 1, 7, 3, 100] {
            pool.insert(E {
                id,
                label: format!("v{id}"),
            })
            .await
            .unwrap();
        }
    });

    let first = pool.export_entries_postcard().unwrap();
    let second = pool.export_entries_postcard().unwrap();
    assert_eq!(first, second, "export must be byte-for-byte deterministic");

    // Decode and confirm id order in body.
    let body_after_header = strip_header(&first, wire::WIRE_FORMAT_MAJOR, KIND_PUNNU_ENTRIES);
    let count = u32::from_le_bytes(body_after_header[..4].try_into().unwrap()) as usize;
    assert_eq!(count, 5);
    let mut cursor = &body_after_header[4..];
    let mut decoded_ids = Vec::with_capacity(count);
    for _ in 0..count {
        let (entry, rest): (E, &[u8]) = postcard::take_from_bytes(cursor).unwrap();
        decoded_ids.push(entry.id);
        cursor = rest;
    }
    assert_eq!(decoded_ids, vec![1, 3, 7, 42, 100]);
}

#[tokio::test]
async fn export_entries_postcard_does_not_require_clone_payload() {
    // Compile-time assertion that NoCloneE has no `Clone` impl — using
    // `assert_not_clone!` would require `static_assertions`; instead
    // we trust the type definition above and the fact that this test
    // both compiles and runs.
    let pool = Punnu::<NoCloneE>::builder().build();
    pool.insert(NoCloneE {
        id: 5,
        label: "five".into(),
    })
    .await
    .unwrap();

    let bytes = pool.export_entries_postcard().unwrap();

    let restored = Punnu::<NoCloneE>::builder().build();
    let stats = restored.restore_entries_postcard(&bytes).unwrap();

    assert_eq!(stats.inserted, 1);
    assert_eq!(restored.get(&5).unwrap().label, "five");
}

#[tokio::test]
async fn restore_entries_postcard_replaces_l1_atomically() {
    let pool = Punnu::<E>::builder().build();
    pool.insert(E {
        id: 1,
        label: "old".into(),
    })
    .await
    .unwrap();
    pool.insert(E {
        id: 2,
        label: "stays".into(),
    })
    .await
    .unwrap();

    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 2,
            label: "fresh".into(),
        })
        .await
        .unwrap();
    donor
        .insert(E {
            id: 3,
            label: "new".into(),
        })
        .await
        .unwrap();
    let bytes = donor.export_entries_postcard().unwrap();

    let stats = pool.restore_entries_postcard(&bytes).unwrap();

    assert_eq!(stats.inserted, 1);
    assert_eq!(stats.updated, 1);
    assert_eq!(stats.removed, 1);
    assert!(
        pool.get(&1).is_none(),
        "id absent from snapshot must be removed"
    );
    assert_eq!(pool.get(&2).unwrap().label, "fresh");
    assert_eq!(pool.get(&3).unwrap().label, "new");
}

#[tokio::test]
async fn restore_entries_postcard_accepts_empty_snapshot() {
    let pool = Punnu::<E>::builder().build();
    pool.insert(E {
        id: 1,
        label: "old".into(),
    })
    .await
    .unwrap();

    let donor: Punnu<E> = Punnu::<E>::builder().build();
    let bytes = donor.export_entries_postcard().unwrap();

    let stats = pool.restore_entries_postcard(&bytes).unwrap();

    assert_eq!(stats.inserted, 0);
    assert_eq!(stats.updated, 0);
    assert_eq!(stats.removed, 1);
    assert!(pool.is_empty());
}

#[tokio::test]
async fn restore_entries_postcard_accepts_exact_lru_size_snapshot() {
    let lru = 4_usize;
    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: lru,
            ..Default::default()
        })
        .build();

    let donor: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: lru,
            ..Default::default()
        })
        .build();
    for id in 1..=lru as i64 {
        donor
            .insert(E {
                id,
                label: format!("v{id}"),
            })
            .await
            .unwrap();
    }
    let bytes = donor.export_entries_postcard().unwrap();

    let stats = pool.restore_entries_postcard(&bytes).unwrap();
    assert_eq!(stats.inserted, lru);
    assert_eq!(pool.len(), lru);
}

#[test]
fn restore_entries_postcard_rejects_type_mismatch_before_mutation() {
    let pool = Punnu::<E>::builder().build();
    let donor: Punnu<F> = Punnu::<F>::builder().build();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        donor.insert(F { id: 9 }).await.unwrap();
    });
    let bytes = donor.export_entries_postcard().unwrap();

    let err = pool.restore_entries_postcard(&bytes).unwrap_err();

    match err {
        PunnuSnapshotError::WireFormat(WireFormatError::TypeNameMismatch { .. }) => {}
        other => panic!("expected wire-format type-name mismatch, got {other:?}"),
    }
    assert!(pool.is_empty());
}

#[test]
fn restore_entries_postcard_rejects_duplicate_ids_before_mutation() {
    let pool = Punnu::<E>::builder().build();
    // Build a snapshot byte stream that intentionally contains the
    // same id twice. Use the wire helpers from outside to avoid
    // depending on private encoders: encode a header with kind 0x03,
    // then a count of 2 followed by two postcard-encoded entries with
    // the same id.
    let mut bytes = Vec::new();
    encode_test_header(&mut bytes, KIND_PUNNU_ENTRIES, E::cache_type_name());
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    let body_a = postcard::to_allocvec(&E {
        id: 1,
        label: "a".into(),
    })
    .unwrap();
    bytes.extend_from_slice(&body_a);
    let body_b = postcard::to_allocvec(&E {
        id: 1,
        label: "b".into(),
    })
    .unwrap();
    bytes.extend_from_slice(&body_b);

    let err = pool.restore_entries_postcard(&bytes).unwrap_err();

    assert!(matches!(err, PunnuSnapshotError::DuplicateId));
    assert!(pool.is_empty());
}

#[test]
fn restore_entries_postcard_rejects_more_than_lru_size_before_mutation() {
    let lru = 2_usize;
    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: lru,
            ..Default::default()
        })
        .build();

    let donor: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: 16,
            ..Default::default()
        })
        .build();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        for id in 1..=3_i64 {
            donor
                .insert(E {
                    id,
                    label: format!("v{id}"),
                })
                .await
                .unwrap();
        }
    });
    let bytes = donor.export_entries_postcard().unwrap();

    let err = pool.restore_entries_postcard(&bytes).unwrap_err();

    match err {
        PunnuSnapshotError::TooManyEntries { entries, limit } => {
            assert_eq!(entries, 3);
            assert_eq!(limit, lru);
        }
        other => panic!("expected TooManyEntries, got {other:?}"),
    }
    assert!(pool.is_empty());
}

#[test]
fn restore_entries_postcard_rejects_more_than_lru_size_before_entry_decode() {
    // Empty snapshot bytes patched so the count claims more entries
    // than the receiving pool's lru_size, with an empty body. Restore
    // must reject as TooManyEntries based on the count alone, not as
    // a postcard codec error.
    let lru = 4_usize;
    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: lru,
            ..Default::default()
        })
        .build();

    let mut bytes = Vec::new();
    encode_test_header(&mut bytes, KIND_PUNNU_ENTRIES, E::cache_type_name());
    let too_many = (lru + 1) as u32;
    bytes.extend_from_slice(&too_many.to_le_bytes());
    // No per-entry postcard bodies; the count check must reject before
    // the decoder gets a chance to fail.

    let err = pool.restore_entries_postcard(&bytes).unwrap_err();

    match err {
        PunnuSnapshotError::TooManyEntries { entries, limit } => {
            assert_eq!(entries, lru + 1);
            assert_eq!(limit, lru);
        }
        other => panic!("expected TooManyEntries, got {other:?}"),
    }
}

#[test]
fn restore_entries_postcard_does_not_panic_on_huge_count_with_empty_body() {
    // A pool may legitimately be configured with a very large
    // `lru_size`, which lets the count <= lru_size guard accept a
    // declared count of `u32::MAX`. With no per-entry body bytes, the
    // decoder must not panic on capacity overflow or abort on an OOM
    // allocation — fallible reservation must surface as a recoverable
    // PunnuSnapshotError::WireFormat(WireFormatError::Codec). The
    // pool's L1 must remain untouched.
    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: u32::MAX as usize,
            ..Default::default()
        })
        .build();

    let mut bytes = Vec::new();
    encode_test_header(&mut bytes, KIND_PUNNU_ENTRIES, E::cache_type_name());
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    // No per-entry body bytes — the snapshot is malformed, but the
    // count alone passes the count <= lru_size check.

    let err = pool.restore_entries_postcard(&bytes).unwrap_err();

    match err {
        PunnuSnapshotError::WireFormat(WireFormatError::Codec(_)) => {}
        other => panic!("expected wire-format codec error, got {other:?}"),
    }
    assert!(pool.is_empty());
}

#[tokio::test]
async fn restore_entries_postcard_rejects_strict_backend_write_in_flight_before_mutation() {
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
                id: 7,
                label: "blocking".into(),
            })
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), put_entered.notified())
        .await
        .expect("strict put should reach backend");

    // Build a small snapshot to attempt restoring while the strict
    // reservation is held.
    let donor: Punnu<E> = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 8,
            label: "snapshot".into(),
        })
        .await
        .unwrap();
    let bytes = donor.export_entries_postcard().unwrap();

    let err = pool.restore_entries_postcard(&bytes).unwrap_err();

    match err {
        PunnuSnapshotError::BackendWriteInFlight { reserved } => {
            assert_eq!(reserved, 1);
        }
        other => panic!("expected BackendWriteInFlight, got {other:?}"),
    }
    assert!(
        pool.get(&8).is_none(),
        "snapshot must not be applied when a strict reservation is in flight"
    );

    put_release.notify_one();
    inserter.await.unwrap().unwrap();
}

#[tokio::test]
async fn restore_entries_postcard_applies_target_default_ttl() {
    tokio::time::pause();

    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 1,
            label: "value".into(),
        })
        .await
        .unwrap();
    let bytes = donor.export_entries_postcard().unwrap();

    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(2)),
            ..Default::default()
        })
        .build();

    let _stats = pool.restore_entries_postcard(&bytes).unwrap();

    assert!(pool.get(&1).is_some(), "entry must be live immediately");
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(
        pool.get(&1).is_none(),
        "entry must respect target pool's default_ttl after restore"
    );
}

#[tokio::test]
async fn restore_entries_postcard_emits_events_after_publish() {
    let pool = Punnu::<E>::builder().build();
    pool.insert(E {
        id: 1,
        label: "stays".into(),
    })
    .await
    .unwrap();
    pool.insert(E {
        id: 2,
        label: "drops".into(),
    })
    .await
    .unwrap();
    let mut rx = pool.events();

    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 1,
            label: "stays".into(),
        })
        .await
        .unwrap();
    donor
        .insert(E {
            id: 3,
            label: "added".into(),
        })
        .await
        .unwrap();
    let bytes = donor.export_entries_postcard().unwrap();

    let stats = pool.restore_entries_postcard(&bytes).unwrap();
    assert_eq!(stats.inserted, 1);
    assert_eq!(stats.removed, 1);

    // After restore returns, L1 must be visible: the snapshot's id 3
    // is present and id 2 is gone. We assert this *before* draining
    // the broadcast — readers can reach the new state without waiting
    // on event delivery.
    assert!(pool.get(&3).is_some());
    assert!(pool.get(&2).is_none());

    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    assert!(
        events.iter().any(|e| matches!(
            e,
            PunnuEvent::Invalidate {
                id: 2,
                reason: EventReason::Manual,
            }
        )),
        "removed id should emit Invalidate(Manual): {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, PunnuEvent::Insert { value } if value.id == 3)),
        "newly inserted id should emit Insert: {events:?}"
    );
}

#[tokio::test]
async fn restore_entries_postcard_does_not_write_through_to_backend() {
    let backend = RecordingBackend::default();
    let put_count = backend.put_count.clone();
    let invalidate_count = backend.invalidate_count.clone();

    let pool: Punnu<E> = Punnu::<E>::builder().backend(backend).build();
    pool.insert(E {
        id: 1,
        label: "old".into(),
    })
    .await
    .unwrap();
    let baseline_puts = put_count.load(Ordering::SeqCst);
    let baseline_invalidates = invalidate_count.load(Ordering::SeqCst);

    let donor: Punnu<E> = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 2,
            label: "fresh".into(),
        })
        .await
        .unwrap();
    let bytes = donor.export_entries_postcard().unwrap();

    pool.restore_entries_postcard(&bytes).unwrap();

    assert_eq!(
        put_count.load(Ordering::SeqCst),
        baseline_puts,
        "restore must not write through to the L2 backend"
    );
    assert_eq!(
        invalidate_count.load(Ordering::SeqCst),
        baseline_invalidates,
        "restore must not invalidate L2 backend entries"
    );
}

#[tokio::test]
async fn restore_entries_postcard_reports_insert_update_remove_counts() {
    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Update,
            ..Default::default()
        })
        .build();
    pool.insert(E {
        id: 1,
        label: "old1".into(),
    })
    .await
    .unwrap();
    pool.insert(E {
        id: 2,
        label: "drops".into(),
    })
    .await
    .unwrap();
    pool.insert(E {
        id: 3,
        label: "old3".into(),
    })
    .await
    .unwrap();

    let donor: Punnu<E> = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 1,
            label: "new1".into(),
        })
        .await
        .unwrap();
    donor
        .insert(E {
            id: 3,
            label: "new3".into(),
        })
        .await
        .unwrap();
    donor
        .insert(E {
            id: 4,
            label: "added".into(),
        })
        .await
        .unwrap();
    let bytes = donor.export_entries_postcard().unwrap();

    let stats = pool.restore_entries_postcard(&bytes).unwrap();

    assert_eq!(stats.inserted, 1, "id 4 only present after restore");
    assert_eq!(stats.updated, 2, "ids 1 and 3 replaced by snapshot");
    assert_eq!(stats.removed, 1, "id 2 absent from snapshot");
}

#[tokio::test(start_paused = true)]
async fn restore_entries_postcard_excludes_expired_residents_from_removed_count() {
    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            default_ttl: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();
    pool.insert(E {
        id: 1,
        label: "expires".into(),
    })
    .await
    .unwrap();
    pool.insert_with_ttl(
        E {
            id: 2,
            label: "long".into(),
        },
        Duration::from_secs(60),
    )
    .await
    .unwrap();

    tokio::time::advance(Duration::from_secs(2)).await;

    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 3,
            label: "added".into(),
        })
        .await
        .unwrap();
    let bytes = donor.export_entries_postcard().unwrap();

    let stats = pool.restore_entries_postcard(&bytes).unwrap();

    assert_eq!(stats.inserted, 1);
    assert_eq!(
        stats.removed, 1,
        "only the live id 2 should be counted as removed; the expired id 1 was already absent to readers"
    );
    assert!(pool.get(&1).is_none());
    assert!(pool.get(&2).is_none());
    assert_eq!(pool.get(&3).unwrap().label, "added");
}

#[tokio::test]
async fn restore_entries_postcard_counts_existing_replacements_as_updated_under_last_write_wins() {
    let pool: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::LastWriteWins,
            ..Default::default()
        })
        .build();
    pool.insert(E {
        id: 1,
        label: "old".into(),
    })
    .await
    .unwrap();

    let mut rx = pool.events();

    let donor = Punnu::<E>::builder().build();
    donor
        .insert(E {
            id: 1,
            label: "new".into(),
        })
        .await
        .unwrap();
    let bytes = donor.export_entries_postcard().unwrap();

    let stats = pool.restore_entries_postcard(&bytes).unwrap();
    assert_eq!(
        stats,
        PunnuRestoreStats {
            inserted: 0,
            updated: 1,
            removed: 0,
        }
    );
    assert_eq!(pool.get(&1).unwrap().label, "new");

    // Drain events: under LastWriteWins, replacement emits Insert.
    let mut sink = Vec::new();
    while let Ok(event) = rx.try_recv() {
        sink.push(event);
    }
    assert!(
        sink.iter()
            .any(|e| matches!(e, PunnuEvent::Insert { value } if value.id == 1)),
        "LastWriteWins replacement should emit Insert event: {sink:?}"
    );
    assert!(
        !sink.iter().any(|e| matches!(e, PunnuEvent::Update { .. })),
        "LastWriteWins should not emit Update events: {sink:?}"
    );
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

const MAGIC: &[u8; 8] = b"SASSI\0W\0";
const KIND_PUNNU_ENTRIES: u8 = 0x03;

fn encode_test_header(out: &mut Vec<u8>, kind: u8, type_name: &str) {
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&wire::WIRE_FORMAT_MAJOR.to_le_bytes());
    out.push(kind);
    out.push(0);
    let name = type_name.as_bytes();
    out.extend_from_slice(&(name.len() as u16).to_le_bytes());
    out.extend_from_slice(name);
}

fn strip_header(bytes: &[u8], expected_major: u16, expected_kind: u8) -> &[u8] {
    assert_eq!(&bytes[..8], MAGIC);
    assert_eq!(u16::from_le_bytes([bytes[8], bytes[9]]), expected_major);
    assert_eq!(bytes[10], expected_kind);
    assert_eq!(bytes[11], 0);
    let name_len = u16::from_le_bytes([bytes[12], bytes[13]]) as usize;
    &bytes[14 + name_len..]
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
}

#[derive(Default)]
struct RecordingBackend {
    put_count: Arc<AtomicUsize>,
    invalidate_count: Arc<AtomicUsize>,
    stored: Arc<Mutex<Vec<(i64, String)>>>,
}

#[async_trait]
impl CacheBackend<E> for RecordingBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<E>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        id: &i64,
        value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.put_count.fetch_add(1, Ordering::SeqCst);
        self.stored
            .lock()
            .expect("recording backend lock poisoned")
            .push((*id, value.label.clone()));
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        self.invalidate_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }

    fn invalidation_stream(&self, _keyspace: BackendKeyspace) -> BackendInvalidationStream<i64> {
        Box::pin(stream::empty())
    }
}
