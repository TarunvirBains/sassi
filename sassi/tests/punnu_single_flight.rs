//! Task 7 — single-flight cancellation contract (spec §3.5.1).
//!
//! Four owner-loss cases must all behave deterministically:
//!
//! 1. Originator dropped, peers polling — fetch stays alive.
//! 2. All awaiters drop — fetch is cancelled, registry slot cleared,
//!    next call retries from cold.
//! 3. Fetcher panics — every awaiter sees `FetcherPanic`.
//! 4. Caller-imposed deadline — composes with `tokio::time::timeout`;
//!    surfaces as case 1 from the registry's perspective.
//!
//! Cases 1, 2, 3 have direct tests here. Case 4 is exercised via the
//! `cancellation_with_peers_keeps_fetch_alive` test (the originator
//! has a short timeout; the peer has a long one).

#![cfg(feature = "runtime-tokio")]

use sassi::{Cacheable, FetchError, Field, Punnu};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
async fn n_concurrent_get_or_fetch_calls_invoke_fetcher_once() {
    let p = Punnu::<E>::builder().build();
    let counter = Arc::new(AtomicUsize::new(0));

    // Spawn 10 racing calls for the same id. Counter increments
    // inside the fetcher; we assert it ends at exactly 1.
    let mut handles = Vec::new();
    for _ in 0..10 {
        let p = p.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            p.get_or_fetch(&1, move |id| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    // Hold the fetch open long enough for all peers
                    // to register before the resolution lands.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<_, FetchError>(Some(E {
                        id,
                        name: "fetched".into(),
                    }))
                }
            })
            .await
        }));
    }

    for h in handles {
        let result = h.await.unwrap().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().name, "fetched");
    }

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "fetcher should be called exactly once across coalesced peers"
    );

    // Result is now in L1 — subsequent calls hit cache, fetcher
    // unchanged.
    let cached = p.get(&1);
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().name, "fetched");
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_with_peers_keeps_fetch_alive() {
    // Case 1 from the cancellation contract: the originator times
    // out via `tokio::time::timeout`, but a peer is still polling
    // and gets the result. The fetcher should run exactly once.
    let p = Punnu::<E>::builder().build();
    let counter = Arc::new(AtomicUsize::new(0));

    let p1 = p.clone();
    let c1 = counter.clone();
    let originator = tokio::spawn(async move {
        // Short timeout — the fetcher takes 100ms, the timeout fires
        // at 20ms. The originator drops its future.
        let _ = tokio::time::timeout(
            Duration::from_millis(20),
            p1.get_or_fetch(&1, move |id| {
                let c1 = c1.clone();
                async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok::<_, FetchError>(Some(E {
                        id,
                        name: "kept-alive".into(),
                    }))
                }
            }),
        )
        .await;
    });

    // Give the originator a moment to register its fetch with the
    // single-flight registry before the peer arrives.
    tokio::time::sleep(Duration::from_millis(5)).await;

    let p2 = p.clone();
    let peer = tokio::spawn(async move {
        // Peer attaches to the same in-flight fetch. Its own fetcher
        // closure must NOT run — coalescing should hand it the
        // existing future.
        p2.get_or_fetch(&1, |_| async {
            panic!("peer's fetcher should not be invoked");
        })
        .await
    });

    let _ = originator.await;
    let result = peer.await.unwrap().unwrap();
    assert!(
        result.is_some(),
        "peer should still see the cached value even after originator timed out"
    );
    assert_eq!(result.unwrap().name, "kept-alive");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "fetcher invoked exactly once across the coalesced peers"
    );
}

#[tokio::test]
async fn all_awaiters_dropped_clears_registry_and_next_call_retries_from_cold() {
    // Case 2: every awaiter drops before the fetch resolves; the
    // registry slot is cleared (via the SlotGuard inside the future),
    // and a subsequent call invokes the fetcher fresh.
    let p = Punnu::<E>::builder().build();
    let counter = Arc::new(AtomicUsize::new(0));

    let p1 = p.clone();
    let c1 = counter.clone();
    // Originator with a short timeout. No peer.
    let originator = tokio::spawn(async move {
        let _ = tokio::time::timeout(
            Duration::from_millis(20),
            p1.get_or_fetch(&1, move |id| {
                let c1 = c1.clone();
                async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    Ok::<_, FetchError>(Some(E {
                        id,
                        name: "first".into(),
                    }))
                }
            }),
        )
        .await;
    });

    // Wait for the originator to drop the future via timeout.
    let _ = originator.await;

    // After all awaiters dropped, the registry slot should be empty.
    // A fresh get_or_fetch must invoke the fetcher again.
    let counter_for_second = counter.clone();
    let result = p
        .get_or_fetch(&1, move |id| {
            let c = counter_for_second.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok::<_, FetchError>(Some(E {
                    id,
                    name: "second".into(),
                }))
            }
        })
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.name, "second");
    // Counter is at least 2 (the second call's fetcher ran). The
    // first call's fetcher may or may not have completed before its
    // future was dropped — the contract says "not your problem"; what
    // matters is that the second call retried from cold.
    assert!(counter.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn fetcher_panic_propagates_as_fetcher_panic_error() {
    // Case 3: fetcher panics; every awaiter sees `FetcherPanic`.
    let p = Punnu::<E>::builder().build();

    let result = p
        .get_or_fetch(&1, |_| async {
            panic!("simulated fetcher panic");
            #[allow(unreachable_code)]
            Ok::<Option<E>, FetchError>(None)
        })
        .await;

    match result {
        Err(FetchError::FetcherPanic { type_name, message }) => {
            assert!(
                type_name.contains("E"),
                "type_name should include the cached type's name; got {type_name}"
            );
            assert_eq!(message, "simulated fetcher panic");
        }
        other => panic!("expected FetcherPanic, got {other:?}"),
    }

    // After a panic, the registry slot is cleared and a subsequent
    // call retries.
    let result = p
        .get_or_fetch(&1, |id| async move {
            Ok::<_, FetchError>(Some(E {
                id,
                name: "after-panic".into(),
            }))
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.name, "after-panic");
}

#[tokio::test]
async fn fetcher_panic_broadcasts_to_all_concurrent_peers() {
    // Multiple peers racing for the same id when the fetcher panics
    // — every peer must see `FetcherPanic`, not a hang or a stale
    // value.
    let p = Punnu::<E>::builder().build();

    let mut handles = Vec::new();
    for _ in 0..5 {
        let p = p.clone();
        handles.push(tokio::spawn(async move {
            p.get_or_fetch(&1, |_id| async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                panic!("broadcast panic");
                #[allow(unreachable_code)]
                Ok::<Option<E>, FetchError>(None)
            })
            .await
        }));
    }

    for h in handles {
        match h.await.unwrap() {
            Err(FetchError::FetcherPanic { message, .. }) => {
                assert_eq!(message, "broadcast panic");
            }
            other => panic!("expected FetcherPanic on every peer, got {other:?}"),
        }
    }
}
