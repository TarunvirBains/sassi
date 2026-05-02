//! `Punnu::get_or_fetch_many` batch path.
//!
//! Spec §3.5: split ids into hits + misses, send one batch fetch
//! for the missing set, merge with hits. Per-id single-flight on
//! individual lookups within the batch.

#![cfg(feature = "runtime-tokio")]

use sassi::{
    Cacheable, EventReason, FetchError, Field, InsertError, OnConflict, Punnu, PunnuConfig,
    PunnuEvent,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast::error::TryRecvError;

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
async fn batch_only_fetches_missing() {
    let p = Punnu::<E>::builder().build();

    // Pre-cache id 1 and 3.
    p.insert(E { id: 1 }).await.unwrap();
    p.insert(E { id: 3 }).await.unwrap();

    let received: Arc<std::sync::Mutex<Vec<i64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let received_for_fetcher = received.clone();
    let result = p
        .get_or_fetch_many(&[1, 2, 3, 4], move |missing| {
            let received = received_for_fetcher.clone();
            async move {
                received.lock().unwrap().extend(missing.iter().copied());
                Ok::<_, FetchError>(missing.into_iter().map(|id| E { id }).collect())
            }
        })
        .await
        .unwrap();

    assert_eq!(result.len(), 4, "all four ids resolved");

    // Fetcher saw only the missing ids — 2 and 4.
    let mut got = received.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec![2, 4]);

    // Both newly-fetched ids landed in L1.
    assert!(p.get(&2).is_some());
    assert!(p.get(&4).is_some());
    assert_eq!(p.len(), 4);
}

#[tokio::test]
async fn empty_missing_returns_only_hits() {
    let p = Punnu::<E>::builder().build();
    p.insert(E { id: 1 }).await.unwrap();
    p.insert(E { id: 2 }).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_for_fetch = counter.clone();

    let result = p
        .get_or_fetch_many(&[1, 2], move |_missing| {
            let counter = counter_for_fetch.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, FetchError>(Vec::new())
            }
        })
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "fetcher must not run when nothing is missing"
    );
}

#[tokio::test]
async fn duplicates_in_input_dedupe_before_fetcher() {
    // Consumers may pass `&[1, 1, 1]`; the batch fetcher should see
    // a deduplicated list (`vec![1]`), and the result should still
    // contain three Arc handles (one per requested id, all pointing
    // to the same value).
    let p = Punnu::<E>::builder().build();
    let received: Arc<std::sync::Mutex<Vec<i64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let received_for_fetcher = received.clone();

    let result = p
        .get_or_fetch_many(&[1, 1, 1], move |missing| {
            let received = received_for_fetcher.clone();
            async move {
                received.lock().unwrap().extend(missing.iter().copied());
                Ok::<_, FetchError>(missing.into_iter().map(|id| E { id }).collect())
            }
        })
        .await
        .unwrap();

    // The fetcher saw only one distinct id.
    let got = received.lock().unwrap().clone();
    assert_eq!(got, vec![1]);
    // The result is non-empty (at least the fetched arc).
    assert!(!result.is_empty());
    // L1 has exactly one entry.
    assert_eq!(p.len(), 1);
}

#[tokio::test]
async fn batch_fetcher_error_propagates() {
    let p = Punnu::<E>::builder().build();

    let result = p
        .get_or_fetch_many(&[1, 2], |_missing| async {
            Err::<Vec<E>, _>(FetchError::Serialization("batch failed".into()))
        })
        .await;

    assert!(matches!(result, Err(FetchError::Serialization(_))));
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn empty_input_returns_empty_without_fetcher() {
    let p = Punnu::<E>::builder().build();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_for_fetch = counter.clone();

    let result = p
        .get_or_fetch_many(&[], move |_missing| {
            let counter = counter_for_fetch.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, FetchError>(Vec::new())
            }
        })
        .await
        .unwrap();

    assert!(result.is_empty());
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn get_or_fetch_many_rejects_unrequested_fetch_id_without_partial_insert() {
    let p = Punnu::<E>::builder().build();

    let result = p
        .get_or_fetch_many(&[1, 2], |_missing| async {
            Ok::<_, FetchError>(vec![E { id: 1 }, E { id: 999 }])
        })
        .await;

    match result {
        Err(FetchError::IdentityMismatch { type_name }) => {
            assert!(
                type_name.contains("E"),
                "type_name should include the cached type's name; got {type_name}"
            );
        }
        other => panic!("expected IdentityMismatch error, got {other:?}"),
    }

    assert!(
        p.get(&1).is_none(),
        "valid fetched id must not be partially inserted"
    );
    assert!(p.get(&2).is_none(), "missing requested id remains absent");
    assert!(p.get(&999).is_none(), "unrequested id must not be cached");
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn get_or_fetch_many_dedupes_duplicate_returned_ids() {
    let p = Punnu::<E>::builder().build();
    let mut rx = p.events();

    let result = p
        .get_or_fetch_many(&[1], |_missing| async {
            Ok::<_, FetchError>(vec![E { id: 1 }, E { id: 1 }])
        })
        .await
        .unwrap();

    assert_eq!(
        result.len(),
        1,
        "duplicate fetched ids should produce one returned Arc"
    );
    assert_eq!(result[0].id, 1);
    assert_eq!(p.len(), 1);
    assert!(p.get(&1).is_some());

    match rx.try_recv().expect("expected one insert event") {
        PunnuEvent::Insert { value } => assert_eq!(value.id, 1),
        other => panic!("expected Insert event, got {other:?}"),
    }
    assert!(
        matches!(rx.try_recv(), Err(TryRecvError::Empty)),
        "duplicate returned ids must not emit duplicate insert events"
    );
}

#[tokio::test]
async fn get_or_fetch_many_conflict_does_not_partially_insert() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Reject,
            ..Default::default()
        })
        .build();
    let mut rx = p.events();
    let p_for_fetcher = p.clone();

    let result = p
        .get_or_fetch_many(&[1, 2], move |_missing| async move {
            p_for_fetcher
                .insert(E { id: 2 })
                .await
                .expect("competing writer should win id 2 while batch fetch awaits");
            Ok::<_, FetchError>(vec![E { id: 1 }, E { id: 2 }])
        })
        .await;

    assert!(
        matches!(result, Err(FetchError::Insert(InsertError::Conflict))),
        "Reject conflict should surface as a fetch insert error; got {result:?}"
    );
    assert!(
        p.get(&1).is_none(),
        "batch must not partially publish id 1 before failing on id 2"
    );
    assert!(
        p.get(&2).is_some(),
        "the competing writer's value should remain resident"
    );
    assert_eq!(p.len(), 1);

    let mut saw_id_1_insert = false;
    let mut saw_id_2_insert = false;
    while let Ok(event) = rx.try_recv() {
        match event {
            PunnuEvent::Insert { value } if value.id == 1 => saw_id_1_insert = true,
            PunnuEvent::Insert { value } if value.id == 2 => saw_id_2_insert = true,
            other => panic!("unexpected event after failed batch insert: {other:?}"),
        }
    }
    assert!(
        !saw_id_1_insert,
        "failed batch must not emit an insert event for a rolled-back id"
    );
    assert!(
        saw_id_2_insert,
        "the competing writer should still have emitted its own insert"
    );
}

#[tokio::test]
async fn get_or_fetch_many_capacity_pressure_emits_only_final_resident_inserts() {
    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            lru_size: 2,
            ..Default::default()
        })
        .build();
    let mut rx = p.events();

    let result = p
        .get_or_fetch_many(&[1, 2, 3, 4], |missing| async move {
            Ok::<_, FetchError>(missing.into_iter().map(|id| E { id }).collect())
        })
        .await
        .unwrap();

    assert_eq!(
        result.len(),
        4,
        "batch callers should receive fetched values even when L1 can retain only a subset"
    );
    assert_eq!(p.len(), 2);
    let mut final_ids = p
        .scope(Vec::new())
        .collect()
        .into_iter()
        .map(|value| value.id)
        .collect::<Vec<_>>();
    final_ids.sort_unstable();

    let mut inserted_ids = Vec::new();
    while let Ok(event) = rx.try_recv() {
        match event {
            PunnuEvent::Insert { value } => inserted_ids.push(value.id),
            PunnuEvent::Invalidate {
                reason: EventReason::LruEvict { .. },
                ..
            } => panic!("transient fetched values must not emit LRU invalidations"),
            other => panic!("unexpected event after batch fetch: {other:?}"),
        }
    }
    inserted_ids.sort_unstable();
    assert_eq!(
        inserted_ids, final_ids,
        "insert events must describe only values retained in the committed snapshot"
    );
}
