#![cfg(all(feature = "serde", feature = "runtime-tokio"))]

use async_trait::async_trait;
use sassi::punnu::config::retry_delay_for_attempt;
use sassi::{
    BackendError, BackendFailureMode, BackendInvalidation, BackendKeyspace, CacheBackend,
    Cacheable, DeltaPunnuFetcher, DeltaQuery, DeltaResult, DeltaSyncCacheable, EventReason,
    FetchError, Field, MemoryBackend, Punnu, PunnuConfig, PunnuFetcher, RefreshMode,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
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

impl DeltaSyncCacheable for E {
    type Watermark = i64;

    fn watermark(&self) -> Self::Watermark {
        self.id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct F {
    id: i64,
    label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StableBackendKey {
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

impl Cacheable for StableBackendKey {
    type Id = i64;
    type Fields = ();

    fn cache_type_name() -> &'static str {
        "sassi.test.StableBackendKey"
    }

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {}
}

#[tokio::test]
async fn memory_backend_round_trips_and_expires_wire_envelope() {
    let backend = MemoryBackend::default();
    let keyspace = keyspace::<E>(None);
    let value = E {
        id: 1,
        label: "one".into(),
    };

    backend
        .put(&keyspace, &value.id(), &value, Some(Duration::ZERO))
        .await
        .unwrap();

    assert_eq!(backend.get(&keyspace, &1_i64).await.unwrap(), None::<E>);

    backend
        .put(&keyspace, &value.id(), &value, None)
        .await
        .unwrap();
    assert_eq!(backend.get(&keyspace, &1_i64).await.unwrap(), Some(value));
}

#[test]
fn retry_delay_uses_capped_exponential_backoff() {
    assert_eq!(retry_delay_for_attempt(1), Duration::ZERO);
    assert_eq!(retry_delay_for_attempt(2), Duration::from_millis(25));
    assert_eq!(retry_delay_for_attempt(3), Duration::from_millis(50));
    assert_eq!(retry_delay_for_attempt(8), Duration::from_millis(1_000));
}

#[test]
#[should_panic(expected = "BackendFailureMode::Retry requires attempts >= 1")]
fn retry_zero_attempts_is_rejected_at_build() {
    let _: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 0 },
            ..Default::default()
        })
        .build();
}

#[tokio::test]
async fn error_mode_backend_insert_failure_does_not_mutate_l1() {
    let backend = FailingPutBackend::default();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            ..Default::default()
        })
        .backend(backend)
        .build();

    let err = punnu
        .insert(E {
            id: 7,
            label: "seven".into(),
        })
        .await
        .unwrap_err();

    assert!(matches!(err, sassi::InsertError::BackendFailed(_)));
    assert!(punnu.get(&7).is_none());
}

#[tokio::test]
async fn retry_mode_exhaustion_keeps_l1_success_after_total_attempts() {
    let backend = FailingPutBackend::default();
    let attempts = backend.put_attempts.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 3 },
            ..Default::default()
        })
        .backend(backend)
        .build();

    let inserted = punnu
        .insert(E {
            id: 8,
            label: "eight".into(),
        })
        .await
        .unwrap();

    assert_eq!(inserted.id, 8);
    assert!(punnu.get(&8).is_some());
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[test]
#[should_panic(expected = "PunnuBuilder::build requires an active Tokio runtime")]
fn backend_build_without_active_tokio_runtime_panics_with_clear_message() {
    let _punnu: Punnu<E> = Punnu::<E>::builder()
        .backend(MemoryBackend::default())
        .build();
}

#[tokio::test]
async fn retry_mode_get_async_succeeds_after_retry_and_caches_l2_hit() {
    let backend = RetryGetBackend::new(GetMode::SucceedAfterNetworkFailures(1));
    let attempts = backend.attempts.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 3 },
            ..Default::default()
        })
        .backend(backend)
        .build();

    let loaded = punnu.get_async(&11).await.unwrap().unwrap();

    assert_eq!(loaded.id, 11);
    assert_eq!(loaded.label, "loaded");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(punnu.get(&11).unwrap().label, "loaded");
}

#[tokio::test]
async fn retry_mode_get_async_exhaustion_falls_back_to_miss_after_total_attempts() {
    let backend = RetryGetBackend::new(GetMode::AlwaysNetwork);
    let attempts = backend.attempts.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 3 },
            ..Default::default()
        })
        .backend(backend)
        .build();

    let loaded = punnu.get_async(&12).await.unwrap();

    assert!(loaded.is_none());
    assert!(punnu.get(&12).is_none());
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn retry_mode_get_async_does_not_retry_non_retryable_errors() {
    let backend = RetryGetBackend::new(GetMode::Serialization);
    let attempts = backend.attempts.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 3 },
            ..Default::default()
        })
        .backend(backend)
        .build();

    let loaded = punnu.get_async(&13).await.unwrap();

    assert!(loaded.is_none());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retry_mode_invalidate_succeeds_after_retry() {
    let backend = RetryInvalidateBackend::new(InvalidateMode::SucceedAfterNetworkFailures(1));
    let attempts = backend.attempts.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 3 },
            ..Default::default()
        })
        .backend(backend)
        .build();

    punnu
        .insert(E {
            id: 14,
            label: "fourteen".into(),
        })
        .await
        .unwrap();
    punnu
        .invalidate(&14, sassi::InvalidationReason::OnDelete)
        .await
        .unwrap();

    assert!(punnu.get(&14).is_none());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn retry_mode_invalidate_exhaustion_stops_after_total_attempts() {
    let backend = RetryInvalidateBackend::new(InvalidateMode::AlwaysNetwork);
    let attempts = backend.attempts.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 3 },
            ..Default::default()
        })
        .backend(backend)
        .build();

    punnu
        .insert(E {
            id: 15,
            label: "fifteen".into(),
        })
        .await
        .unwrap();
    punnu
        .invalidate(&15, sassi::InvalidationReason::OnDelete)
        .await
        .unwrap();

    assert!(punnu.get(&15).is_none());
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn retry_mode_invalidate_does_not_retry_non_retryable_errors() {
    let backend = RetryInvalidateBackend::new(InvalidateMode::Serialization);
    let attempts = backend.attempts.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 3 },
            ..Default::default()
        })
        .backend(backend)
        .build();

    punnu
        .insert(E {
            id: 16,
            label: "sixteen".into(),
        })
        .await
        .unwrap();
    punnu
        .invalidate(&16, sassi::InvalidationReason::OnDelete)
        .await
        .unwrap();

    assert!(punnu.get(&16).is_none());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn error_mode_invalidate_failure_returns_error_and_keeps_l1() {
    let backend = RetryInvalidateBackend::new(InvalidateMode::AlwaysNetwork);
    let attempts = backend.attempts.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            ..Default::default()
        })
        .backend(backend)
        .build();

    punnu
        .insert(E {
            id: 17,
            label: "seventeen".into(),
        })
        .await
        .unwrap();

    let err = punnu
        .invalidate(&17, sassi::InvalidationReason::OnDelete)
        .await
        .unwrap_err();

    assert!(matches!(err, BackendError::Network(_)));
    assert_eq!(punnu.get(&17).unwrap().label, "seventeen");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn error_mode_reject_insert_does_not_let_concurrent_writer_win_during_backend_put() {
    let backend = BlockingStrictPutBackend::default();
    let first_put_entered = backend.first_put_entered.clone();
    let first_put_release = backend.first_put_release.clone();
    let second_put_entered = backend.second_put_entered.clone();
    let stored = backend.stored.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            on_conflict: sassi::OnConflict::Reject,
            ..Default::default()
        })
        .backend(backend)
        .build();

    let first = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .insert(E {
                    id: 18,
                    label: "first".into(),
                })
                .await
        })
    };
    first_put_entered.notified().await;

    let second = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .insert(E {
                    id: 18,
                    label: "second".into(),
                })
                .await
        })
    };

    assert!(
        tokio::time::timeout(Duration::from_millis(50), second_put_entered.notified())
            .await
            .is_err(),
        "second writer reached the backend while the first strict insert was in flight"
    );

    first_put_release.notify_one();
    let first_result = first.await.unwrap().unwrap();
    let second_result = second.await.unwrap().unwrap_err();

    assert_eq!(first_result.label, "first");
    assert!(matches!(second_result, sassi::InsertError::Conflict));
    assert_eq!(punnu.get(&18).unwrap().label, "first");
    assert_eq!(stored.lock().unwrap().as_slice(), ["first"]);
}

#[tokio::test]
async fn error_mode_reject_insert_does_not_conflict_with_lazy_fetch_during_backend_put() {
    let backend = BlockingStrictPutBackend::default();
    let first_put_entered = backend.first_put_entered.clone();
    let first_put_release = backend.first_put_release.clone();
    let stored = backend.stored.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            on_conflict: sassi::OnConflict::Reject,
            ..Default::default()
        })
        .backend(backend)
        .build();

    let first = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .insert(E {
                    id: 19,
                    label: "first".into(),
                })
                .await
        })
    };
    first_put_entered.notified().await;

    let fetch = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .get_or_fetch(&19, |id| async move {
                    Ok::<_, sassi::FetchError>(Some(E {
                        id,
                        label: "fetched".into(),
                    }))
                })
                .await
        })
    };

    assert!(
        tokio::time::timeout(Duration::from_millis(50), async {
            while !fetch.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err(),
        "lazy fetch completed while a strict insert reserved the same id"
    );

    first_put_release.notify_one();
    let first_result = first.await.unwrap().unwrap();
    let fetch_result = fetch.await.unwrap().unwrap().unwrap();

    assert_eq!(first_result.label, "first");
    assert_eq!(fetch_result.label, "first");
    assert_eq!(punnu.get(&19).unwrap().label, "first");
    assert_eq!(stored.lock().unwrap().as_slice(), ["first"]);
}

#[tokio::test]
async fn error_mode_invalidate_waits_for_same_id_strict_insert_before_mutating_backend() {
    let backend = BlockingStrictPutAfterStoreBackend::default();
    let put_stored = backend.put_stored.clone();
    let put_release = backend.put_release.clone();
    let invalidate_entered = backend.invalidate_entered.clone();
    let stored = backend.stored.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            ..Default::default()
        })
        .backend(backend)
        .build();

    let insert = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .insert(E {
                    id: 20,
                    label: "fresh".into(),
                })
                .await
        })
    };
    put_stored.notified().await;

    assert_eq!(stored.lock().unwrap().as_deref(), Some("fresh"));
    assert!(punnu.get(&20).is_none());

    let invalidate = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .invalidate(&20, sassi::InvalidationReason::OnDelete)
                .await
        })
    };

    assert!(
        tokio::time::timeout(Duration::from_millis(50), invalidate_entered.notified())
            .await
            .is_err(),
        "strict invalidation reached the backend while the same-id insert was still reserved"
    );

    put_release.notify_one();
    let inserted = insert.await.unwrap().unwrap();
    assert_eq!(inserted.label, "fresh");
    invalidate.await.unwrap().unwrap();

    assert!(punnu.get(&20).is_none());
    assert_eq!(stored.lock().unwrap().as_deref(), None);
}

#[tokio::test]
async fn error_mode_apply_delta_skips_id_reserved_by_strict_insert_in_flight() {
    let backend = BlockingStrictPutAfterStoreBackend::default();
    let put_stored = backend.put_stored.clone();
    let put_release = backend.put_release.clone();
    let stored = backend.stored.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            on_conflict: sassi::OnConflict::Reject,
            ..Default::default()
        })
        .backend(backend)
        .build();

    let insert = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .insert(E {
                    id: 21,
                    label: "strict".into(),
                })
                .await
        })
    };
    put_stored.notified().await;

    let stats = punnu.apply_delta(DeltaResult::new(
        vec![E {
            id: 21,
            label: "delta".into(),
        }],
        HashSet::new(),
    ));

    assert_eq!(stats.applied_items, 0);
    assert_eq!(stats.backend_reserved_skips, 1);
    assert!(punnu.get(&21).is_none());
    assert_eq!(stored.lock().unwrap().as_deref(), Some("strict"));

    put_release.notify_one();
    let inserted = insert.await.unwrap().unwrap();
    assert_eq!(inserted.label, "strict");
    assert_eq!(punnu.get(&21).unwrap().label, "strict");
}

#[tokio::test]
async fn error_mode_refresh_skips_id_reserved_by_strict_insert_in_flight() {
    let backend = BlockingStrictPutAfterStoreBackend::default();
    let put_stored = backend.put_stored.clone();
    let put_release = backend.put_release.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            on_conflict: sassi::OnConflict::Reject,
            ..Default::default()
        })
        .backend(backend)
        .build();

    let insert = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .insert(E {
                    id: 22,
                    label: "strict".into(),
                })
                .await
        })
    };
    put_stored.notified().await;

    let refresh = punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        StaticRefreshFetcher {
            items: vec![E {
                id: 22,
                label: "refresh".into(),
            }],
        },
        RefreshMode::UpsertOnly,
    );
    let refresh_now = refresh.refresh_now();
    tokio::pin!(refresh_now);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), refresh_now.as_mut())
            .await
            .is_err(),
        "refresh completed while a strict insert still reserved the same id"
    );

    put_release.notify_one();
    refresh_now.await.unwrap();
    refresh.cancel();

    let inserted = insert.await.unwrap().unwrap();
    assert_eq!(inserted.label, "strict");
    assert_eq!(punnu.get(&22).unwrap().label, "strict");
}

#[tokio::test]
async fn error_mode_delta_refresh_does_not_advance_watermark_for_reserved_tombstone() {
    let backend = BlockingStrictPutAfterStoreBackend::default();
    let put_stored = backend.put_stored.clone();
    let put_release = backend.put_release.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            on_conflict: sassi::OnConflict::Reject,
            ..Default::default()
        })
        .backend(backend)
        .build();
    let fetcher = StaticDeltaFetcher {
        outcomes: Arc::new(Mutex::new(vec![
            DeltaResult::with_high_watermark(Vec::new(), HashSet::from([23]), 90),
            DeltaResult::with_high_watermark(Vec::new(), HashSet::from([23]), 90),
        ])),
    };
    let handle = punnu.start_delta_refresh(Duration::from_secs(3600), fetcher);

    let insert = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .insert(E {
                    id: 23,
                    label: "strict".into(),
                })
                .await
        })
    };
    put_stored.notified().await;

    let err = handle.update().await.unwrap_err();
    assert!(matches!(err, FetchError::Serialization(_)));
    assert_eq!(handle.watermark(), None);
    assert!(punnu.get(&23).is_none());

    put_release.notify_one();
    let inserted = insert.await.unwrap().unwrap();
    assert_eq!(inserted.label, "strict");

    let retry = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(retry.watermark, Some(90));
    assert!(punnu.get(&23).is_none());
}

#[tokio::test]
async fn error_mode_delta_refresh_rolls_back_membership_when_strict_reservation_defers_apply() {
    let backend = BlockingStrictPutAfterStoreBackend::default();
    let put_stored = backend.put_stored.clone();
    let put_release = backend.put_release.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            on_conflict: sassi::OnConflict::Reject,
            lru_size: 1,
            ..Default::default()
        })
        .backend(backend)
        .build();
    let fetcher = StaticDeltaFetcher {
        outcomes: Arc::new(Mutex::new(vec![DeltaResult::with_high_watermark(
            vec![E {
                id: 24,
                label: "delta".into(),
            }],
            HashSet::new(),
            100,
        )])),
    };
    let handle = punnu
        .start_delta_refresh(Duration::from_secs(3600), fetcher)
        .with_eviction_recovery(true);

    let insert = {
        let punnu = punnu.clone();
        tokio::spawn(async move {
            punnu
                .insert(E {
                    id: 24,
                    label: "strict".into(),
                })
                .await
        })
    };
    put_stored.notified().await;

    let err = handle.update().await.unwrap_err();
    assert!(matches!(err, FetchError::Serialization(_)));
    assert_eq!(handle.pending_eviction_recovery_count(), 0);

    put_release.notify_one();
    insert.await.unwrap().unwrap();

    put_release.notify_one();
    punnu
        .insert(E {
            id: 25,
            label: "evictor".into(),
        })
        .await
        .unwrap();

    let recovery_was_queued = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            if handle.pending_eviction_recovery_count() != 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    handle.cancel();

    assert!(
        !recovery_was_queued,
        "failed delta apply should not leave membership that queues later LRU recovery"
    );
}

#[tokio::test]
async fn get_async_rejects_backend_identity_mismatch_without_inserting() {
    let punnu = Punnu::<E>::builder().backend(WrongIdBackend).build();

    let err = punnu.get_async(&1).await.unwrap_err();

    assert!(matches!(err, BackendError::Serialization(_)));
    assert!(punnu.get(&1).is_none());
    assert!(punnu.get(&999).is_none());
}

#[tokio::test]
async fn backend_invalidation_stream_removes_l1_and_emits_backend_reason() {
    let (backend, tx) = StreamingBackend::new();
    let punnu = Punnu::<E>::builder().backend(backend).build();
    let mut events = punnu.events();

    punnu
        .insert(E {
            id: 1,
            label: "one".into(),
        })
        .await
        .unwrap();
    punnu
        .insert(E {
            id: 2,
            label: "two".into(),
        })
        .await
        .unwrap();
    drain_ready_events(&mut events);

    tx.unbounded_send(Ok(BackendInvalidation::Id(1))).unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        event,
        sassi::PunnuEvent::Invalidate {
            id: 1,
            reason: EventReason::BackendInvalidation { .. }
        }
    ));
    assert!(punnu.get(&1).is_none());
    assert!(punnu.get(&2).is_some());

    tx.unbounded_send(Ok(BackendInvalidation::All)).unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        event,
        sassi::PunnuEvent::Invalidate {
            id: 2,
            reason: EventReason::BackendInvalidation { .. }
        }
    ));
    assert!(punnu.get(&2).is_none());
}

#[tokio::test]
async fn punnu_backend_keyspace_uses_config_namespace_and_type_name() {
    let backend = RecordingBackend::default();
    let seen = backend.seen.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("tenant-a".into()),
            ..Default::default()
        })
        .backend(backend)
        .build();

    punnu
        .insert(E {
            id: 3,
            label: "three".into(),
        })
        .await
        .unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].namespace.as_deref(), Some("tenant-a"));
    assert_eq!(seen[0].type_name, std::any::type_name::<E>());
}

#[tokio::test]
async fn punnu_backend_keyspace_uses_cacheable_type_name_not_rust_type_path() {
    let backend = RecordingBackendForStableKey::default();
    let seen = backend.seen.clone();
    let punnu = Punnu::<StableBackendKey>::builder()
        .backend(backend)
        .build();

    punnu.insert(StableBackendKey { id: 4 }).await.unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].type_name, "sassi.test.StableBackendKey");
    assert_ne!(seen[0].type_name, std::any::type_name::<StableBackendKey>());
}

#[tokio::test]
async fn backend_invalidation_all_is_namespace_scoped_per_punnu() {
    let (backend_a, tx_a) = StreamingBackend::new();
    let (backend_b, _tx_b) = StreamingBackend::new();
    let punnu_a = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("env-a".into()),
            ..Default::default()
        })
        .backend(backend_a)
        .build();
    let punnu_b = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("env-b".into()),
            ..Default::default()
        })
        .backend(backend_b)
        .build();

    punnu_a
        .insert(E {
            id: 1,
            label: "a".into(),
        })
        .await
        .unwrap();
    punnu_b
        .insert(E {
            id: 1,
            label: "b".into(),
        })
        .await
        .unwrap();

    tx_a.unbounded_send(Ok(BackendInvalidation::All)).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while punnu_a.get(&1).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(punnu_a.get(&1).is_none());
    assert_eq!(punnu_b.get(&1).unwrap().label, "b");
}

#[tokio::test]
async fn backend_invalidation_all_is_type_scoped_per_punnu() {
    let (backend_e, tx_e) = StreamingBackend::new();
    let (backend_f, _tx_f) = StreamingBackendForF::new();
    let punnu_e = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("shared-env".into()),
            ..Default::default()
        })
        .backend(backend_e)
        .build();
    let punnu_f = Punnu::<F>::builder()
        .config(PunnuConfig {
            namespace: Some("shared-env".into()),
            ..Default::default()
        })
        .backend(backend_f)
        .build();

    punnu_e
        .insert(E {
            id: 1,
            label: "e".into(),
        })
        .await
        .unwrap();
    punnu_f
        .insert(F {
            id: 1,
            label: "f".into(),
        })
        .await
        .unwrap();

    tx_e.unbounded_send(Ok(BackendInvalidation::All)).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while punnu_e.get(&1).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(punnu_e.get(&1).is_none());
    assert_eq!(punnu_f.get(&1).unwrap().label, "f");
}

#[tokio::test]
async fn backend_invalidation_listener_exits_after_owner_drop_even_when_stream_is_idle() {
    let backend = IdleStreamBackend::default();
    let subscribed = backend.subscribed.clone();
    let stream_dropped = backend.stream_dropped.clone();
    let punnu = Punnu::<E>::builder().backend(backend).build();

    tokio::time::timeout(Duration::from_secs(1), subscribed.notified())
        .await
        .expect("listener should subscribe to backend stream");

    drop(punnu);

    tokio::time::timeout(Duration::from_millis(300), stream_dropped.notified())
        .await
        .expect("listener should drop an idle backend stream after owner-loss");
}

fn keyspace<T: Cacheable>(namespace: Option<&str>) -> BackendKeyspace {
    BackendKeyspace {
        namespace: namespace.map(Arc::from),
        type_name: std::any::type_name::<T>(),
    }
}

fn drain_ready_events(rx: &mut tokio::sync::broadcast::Receiver<sassi::PunnuEvent<E>>) {
    while rx.try_recv().is_ok() {}
}

struct StaticRefreshFetcher {
    items: Vec<E>,
}

#[async_trait]
impl PunnuFetcher<E> for StaticRefreshFetcher {
    async fn fetch(&self) -> Result<Vec<E>, FetchError> {
        Ok(self.items.clone())
    }
}

struct StaticDeltaFetcher {
    outcomes: Arc<Mutex<Vec<DeltaResult<E, i64>>>>,
}

#[async_trait]
impl DeltaPunnuFetcher<E> for StaticDeltaFetcher {
    async fn fetch_delta(&self, _query: DeltaQuery<E>) -> Result<DeltaResult<E, i64>, FetchError> {
        let mut outcomes = self.outcomes.lock().unwrap();
        if outcomes.is_empty() {
            Ok(DeltaResult::new(Vec::new(), HashSet::new()))
        } else {
            Ok(outcomes.remove(0))
        }
    }
}

#[derive(Default)]
struct FailingPutBackend {
    put_attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl CacheBackend<E> for FailingPutBackend {
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
        self.put_attempts.fetch_add(1, Ordering::SeqCst);
        Err(BackendError::Network("down".into()))
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum GetMode {
    SucceedAfterNetworkFailures(usize),
    AlwaysNetwork,
    Serialization,
}

struct RetryGetBackend {
    attempts: Arc<AtomicUsize>,
    mode: GetMode,
}

impl RetryGetBackend {
    fn new(mode: GetMode) -> Self {
        Self {
            attempts: Arc::new(AtomicUsize::new(0)),
            mode,
        }
    }
}

#[async_trait]
impl CacheBackend<E> for RetryGetBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, id: &i64) -> Result<Option<E>, BackendError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        match self.mode {
            GetMode::SucceedAfterNetworkFailures(failures) if attempt <= failures => {
                Err(BackendError::Network("temporary outage".into()))
            }
            GetMode::SucceedAfterNetworkFailures(_) => Ok(Some(E {
                id: *id,
                label: "loaded".into(),
            })),
            GetMode::AlwaysNetwork => Err(BackendError::Network("down".into())),
            GetMode::Serialization => Err(BackendError::Serialization("bad payload".into())),
        }
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum InvalidateMode {
    SucceedAfterNetworkFailures(usize),
    AlwaysNetwork,
    Serialization,
}

struct RetryInvalidateBackend {
    attempts: Arc<AtomicUsize>,
    mode: InvalidateMode,
}

impl RetryInvalidateBackend {
    fn new(mode: InvalidateMode) -> Self {
        Self {
            attempts: Arc::new(AtomicUsize::new(0)),
            mode,
        }
    }
}

#[async_trait]
impl CacheBackend<E> for RetryInvalidateBackend {
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
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        match self.mode {
            InvalidateMode::SucceedAfterNetworkFailures(failures) if attempt <= failures => {
                Err(BackendError::Network("temporary outage".into()))
            }
            InvalidateMode::SucceedAfterNetworkFailures(_) => Ok(()),
            InvalidateMode::AlwaysNetwork => Err(BackendError::Network("down".into())),
            InvalidateMode::Serialization => {
                Err(BackendError::Serialization("bad invalidation".into()))
            }
        }
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}

#[derive(Default)]
struct BlockingStrictPutBackend {
    put_attempts: AtomicUsize,
    first_put_entered: Arc<Notify>,
    first_put_release: Arc<Notify>,
    second_put_entered: Arc<Notify>,
    stored: Arc<Mutex<Vec<String>>>,
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
        value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let attempt = self.put_attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == 1 {
            self.first_put_entered.notify_one();
            self.first_put_release.notified().await;
        } else {
            self.second_put_entered.notify_one();
        }
        self.stored.lock().unwrap().push(value.label.clone());
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
struct BlockingStrictPutAfterStoreBackend {
    put_stored: Arc<Notify>,
    put_release: Arc<Notify>,
    invalidate_entered: Arc<Notify>,
    stored: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl CacheBackend<E> for BlockingStrictPutAfterStoreBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<E>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
        value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        *self.stored.lock().unwrap() = Some(value.label.clone());
        self.put_stored.notify_one();
        self.put_release.notified().await;
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        self.invalidate_entered.notify_one();
        *self.stored.lock().unwrap() = None;
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}

struct WrongIdBackend;

#[async_trait]
impl CacheBackend<E> for WrongIdBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<E>, BackendError> {
        Ok(Some(E {
            id: 999,
            label: "wrong".into(),
        }))
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}

type InvalidationRx =
    futures::channel::mpsc::UnboundedReceiver<Result<BackendInvalidation<i64>, BackendError>>;

struct StreamingBackend {
    rx: Mutex<Option<InvalidationRx>>,
}

impl StreamingBackend {
    fn new() -> (
        Self,
        futures::channel::mpsc::UnboundedSender<Result<BackendInvalidation<i64>, BackendError>>,
    ) {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        (
            Self {
                rx: Mutex::new(Some(rx)),
            },
            tx,
        )
    }
}

#[derive(Default)]
struct RecordingBackend {
    seen: Arc<Mutex<Vec<BackendKeyspaceSnapshot>>>,
}

#[derive(Debug, Clone)]
struct BackendKeyspaceSnapshot {
    namespace: Option<String>,
    type_name: &'static str,
}

impl From<&BackendKeyspace> for BackendKeyspaceSnapshot {
    fn from(value: &BackendKeyspace) -> Self {
        Self {
            namespace: value.namespace.as_ref().map(ToString::to_string),
            type_name: value.type_name,
        }
    }
}

#[async_trait]
impl CacheBackend<E> for RecordingBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<E>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.seen.lock().unwrap().push(keyspace.into());
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
struct RecordingBackendForStableKey {
    seen: Arc<Mutex<Vec<BackendKeyspaceSnapshot>>>,
}

#[async_trait]
impl CacheBackend<StableBackendKey> for RecordingBackendForStableKey {
    async fn get(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
    ) -> Result<Option<StableBackendKey>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &StableBackendKey,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.seen.lock().unwrap().push(keyspace.into());
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}

type InvalidationRxF =
    futures::channel::mpsc::UnboundedReceiver<Result<BackendInvalidation<i64>, BackendError>>;

struct StreamingBackendForF {
    rx: Mutex<Option<InvalidationRxF>>,
}

impl StreamingBackendForF {
    fn new() -> (
        Self,
        futures::channel::mpsc::UnboundedSender<Result<BackendInvalidation<i64>, BackendError>>,
    ) {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        (
            Self {
                rx: Mutex::new(Some(rx)),
            },
            tx,
        )
    }
}

#[async_trait]
impl CacheBackend<F> for StreamingBackendForF {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<F>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &F,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }

    fn invalidation_stream(
        &self,
        _keyspace: BackendKeyspace,
    ) -> sassi::BackendInvalidationStream<i64> {
        Box::pin(
            self.rx
                .lock()
                .unwrap()
                .take()
                .expect("stream should be subscribed once"),
        )
    }
}

#[async_trait]
impl CacheBackend<E> for StreamingBackend {
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
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }

    fn invalidation_stream(
        &self,
        _keyspace: BackendKeyspace,
    ) -> sassi::BackendInvalidationStream<i64> {
        Box::pin(
            self.rx
                .lock()
                .unwrap()
                .take()
                .expect("stream should be subscribed once"),
        )
    }
}

#[derive(Default)]
struct IdleStreamBackend {
    subscribed: Arc<Notify>,
    stream_dropped: Arc<Notify>,
}

struct PendingInvalidationStream {
    dropped: Arc<Notify>,
}

impl Drop for PendingInvalidationStream {
    fn drop(&mut self) {
        self.dropped.notify_waiters();
    }
}

impl futures::Stream for PendingInvalidationStream {
    type Item = Result<BackendInvalidation<i64>, BackendError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

#[async_trait]
impl CacheBackend<E> for IdleStreamBackend {
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
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }

    fn invalidation_stream(
        &self,
        _keyspace: BackendKeyspace,
    ) -> sassi::BackendInvalidationStream<i64> {
        self.subscribed.notify_one();
        Box::pin(PendingInvalidationStream {
            dropped: self.stream_dropped.clone(),
        })
    }
}
