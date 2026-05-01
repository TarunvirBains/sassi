//! Task 7 — `Punnu::get_or_fetch` happy path + miss + error variants.
//!
//! Spec §3.5 describes the lazy-fetch-on-miss pattern: on L1 hit,
//! the fetcher is not invoked; on miss, the fetcher's `Some(value)`
//! lands in L1 (so a subsequent `get` is a hit) and `None` propagates
//! without caching. The single-flight cancellation contract lives in
//! `punnu_single_flight.rs`; this file pins the non-coalescing
//! semantics.

#![cfg(feature = "runtime-tokio")]

use sassi::{Cacheable, FetchError, Field, Punnu};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
struct E {
    id: i64,
    name: String,
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
async fn fetcher_called_on_miss_value_cached() {
    let p = Punnu::<E>::builder().build();
    let counter = Arc::new(AtomicUsize::new(0));

    let counter_for_fetch = counter.clone();
    let result = p
        .get_or_fetch(&1, move |id| {
            let counter = counter_for_fetch.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, FetchError>(Some(E {
                    id,
                    name: "fetched".into(),
                }))
            }
        })
        .await
        .unwrap();

    assert!(result.is_some());
    assert_eq!(result.unwrap().name, "fetched");
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // Subsequent get is an L1 hit — fetcher not called again.
    let cached = p.get(&1);
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().name, "fetched");

    // get_or_fetch on the cached id also doesn't re-invoke.
    let cached_again = p
        .get_or_fetch(&1, |_| async {
            panic!("fetcher must not run for cached id");
        })
        .await
        .unwrap();
    assert!(cached_again.is_some());
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fetcher_returning_none_caches_nothing() {
    let p = Punnu::<E>::builder().build();
    let counter = Arc::new(AtomicUsize::new(0));

    let counter_for_fetch = counter.clone();
    let result = p
        .get_or_fetch(&999, move |_id| {
            let counter = counter_for_fetch.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, FetchError>(None)
            }
        })
        .await
        .unwrap();

    assert!(result.is_none());
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // Cache stays empty — None doesn't get stored.
    assert!(p.get(&999).is_none());
    assert_eq!(p.len(), 0);

    // A second call retries (no negative-cache; that's a future-task
    // enhancement if needed).
    let counter_for_second = counter.clone();
    let _ = p
        .get_or_fetch(&999, move |_id| {
            let counter = counter_for_second.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, FetchError>(None)
            }
        })
        .await
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn fetcher_error_propagates() {
    let p = Punnu::<E>::builder().build();

    let result = p
        .get_or_fetch(&1, |_id| async {
            Err::<Option<E>, _>(FetchError::Serialization("simulated failure".into()))
        })
        .await;

    match result {
        Err(FetchError::Serialization(msg)) => {
            assert_eq!(msg, "simulated failure");
        }
        other => panic!("expected Serialization error, got {other:?}"),
    }

    // Cache untouched on error.
    assert!(p.get(&1).is_none());
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn fetcher_custom_error_propagates() {
    let p = Punnu::<E>::builder().build();

    #[derive(Debug)]
    struct MyErr(&'static str);
    impl std::fmt::Display for MyErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for MyErr {}

    let result = p
        .get_or_fetch(&1, |_id| async {
            Err::<Option<E>, _>(FetchError::Custom(Box::new(MyErr("custom failure"))))
        })
        .await;

    match result {
        Err(FetchError::Custom(e)) => {
            assert_eq!(format!("{e}"), "custom failure");
        }
        other => panic!("expected Custom error, got {other:?}"),
    }
}
