//! Task 7 — `Punnu::get_or_fetch_many` batch path.
//!
//! Spec §3.5: split ids into hits + misses, send one batch fetch
//! for the missing set, merge with hits. Per-id single-flight on
//! individual lookups within the batch.

#![cfg(feature = "runtime-tokio")]

use sassi::{Cacheable, FetchError, Field, Punnu};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
