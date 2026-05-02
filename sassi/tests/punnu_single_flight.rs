//! single-flight cancellation contract (spec §3.5.1).
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

/// Proof of the BLOCK-M1 fix: the L1 insert (and its `PunnuEvent::Insert`
/// emission, TTL deadline construction, and `OnConflict` policy
/// evaluation) must fire **exactly once** per fetch — not once per
/// coalesced peer.
///
/// Before this fix, every awaiter ran `Punnu::insert_arc_into_l1`
/// after the shared future resolved, so 5 coalesced peers fired 5
/// Insert events for one fetched value. Worse, with
/// `OnConflict::Reject`, peers 2..5 would have seen `Conflict`
/// despite a successful shared fetch.
///
/// The fix moves the insert *inside* the shared future body via the
/// `on_fetched` callback. Coalesced peers receive the canonical
/// `Arc<T>` from the same Shared output without re-running the
/// insert.
#[tokio::test]
async fn coalesced_awaiters_fire_insert_event_exactly_once() {
    use sassi::PunnuEvent;

    let p = Punnu::<E>::builder().build();
    let mut events = p.events();

    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(5);
    for _ in 0..5 {
        let p_clone = p.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            p_clone
                .get_or_fetch(&1, move |id| {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        // Sleep so the peers have time to attach.
                        tokio::time::sleep(Duration::from_millis(20)).await;
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
        assert!(result.is_some(), "all peers see the canonical value");
    }

    // Fetcher invoked exactly once (single-flight) — already covered by
    // `n_concurrent_get_or_fetch_calls_invoke_fetcher_once` above; we
    // assert it again here for clarity.
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "fetcher invoked exactly once across coalesced peers"
    );

    // The headline assertion: exactly one Insert event for id=1.
    // Without the M1 fix, this would fire 5 times.
    let mut insert_count = 0;
    while let Ok(ev) = events.try_recv() {
        if let PunnuEvent::Insert { value } = ev
            && value.id == 1
        {
            insert_count += 1;
        }
    }
    assert_eq!(
        insert_count, 1,
        "5 coalesced get_or_fetch calls must fire exactly one Insert event \
         (not one per peer); proof of BLOCK-M1 fix"
    );

    // After the fetch, the cache has the value — subsequent get is a
    // direct hit (no fetcher invocation, no further events).
    let cached = p.get(&1).expect("entry cached after fetch");
    assert_eq!(cached.name, "fetched");
}

/// Proof of the BLOCK-M2 fix: expired-conflict during a fetch must
/// not leave the cache empty.
///
/// Multi-actor scenario the M2 BLOCK actually requires:
///
/// 1. Punnu configured with `OnConflict::Reject` + `default_ttl = 1s`.
/// 2. Actor A starts a `get_or_fetch` for id=1. Initial `get(1)` is a
///    miss (cache empty), so A registers an in-flight fetch and the
///    fetcher closure runs. The fetcher awaits a Notify so the test
///    can interleave Actor B's work before on_fetched fires.
/// 3. Actor B independently `insert`s id=1 with the Punnu's default
///    TTL (1s). The entry is now in L1.
/// 4. Time advances past 1s. Actor B's entry is expired but still
///    resident — lazy `get` observes expiry without physical cleanup.
/// 5. The test signals A's fetcher to complete; A's on_fetched runs
///    against an L1 that has Actor B's expired entry.
///
/// Pre-fix code path: on_fetched calls `insert_arc_into_l1`. Under
/// `OnConflict::Reject`, the expired-but-present entry triggers
/// `Conflict`. The fall-back `punnu.get(&id)` observes the expired
/// entry as a miss, and `unwrap_or(arc)` returns A's never-inserted
/// value. A subsequent `get(1)` still misses — violation of
/// `get_or_fetch`'s post-condition.
///
/// Post-fix code path: on_fetched calls `insert_arc_or_existing`,
/// which coordinates with writers, sees the expired entry in the
/// current snapshot, treats it as absent, and publishes a replacement
/// snapshot with A's value. Subsequent `get(1)` hits with A's value.
///
/// The yield-and-Notify orchestration is what makes the test
/// genuinely exercise on_fetched's expired-conflict path. A
/// single-actor test (the previous version) gets short-circuited by
/// `get_or_fetch`'s initial `get`, so the M2 race window doesn't open.
#[tokio::test(start_paused = true)]
async fn expired_concurrent_insert_does_not_block_single_flight_insert() {
    use sassi::{OnConflict, PunnuConfig};
    use tokio::sync::Notify;

    let p = Punnu::<E>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Reject,
            default_ttl: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build();

    // Step 2: Actor A starts get_or_fetch with a fetcher that awaits
    // a Notify. Cache is empty → initial get(1) misses → registers
    // in-flight fetch → fetcher runs → fetcher parks on Notify.
    //
    // The fetcher fires `fetcher_started_tx` *before* awaiting `go`,
    // giving the test a deterministic readiness signal: by the time
    // the test sees the oneshot resolve, Actor A has definitely (a)
    // called the initial `self.get(id)` (cache miss), (b) registered
    // the single-flight entry, and (c) entered the fetcher body.
    // Actor A is parked on `go.notified()`. Without this, a
    // `yield_now()` heuristic could let Actor B's insert beat Actor
    // A's initial `get`, so Actor A could observe B's value or miss
    // before the intended on_fetched interleaving — making the test
    // vacuous against pre-fix code. Same shape as the issue #4
    // sweep handshake: deterministic readiness > yield count.
    let go = Arc::new(Notify::new());
    let go_for_fetch = go.clone();
    let p_for_fetch = p.clone();
    let (fetcher_started_tx, fetcher_started_rx) = tokio::sync::oneshot::channel::<()>();
    let actor_a = tokio::spawn(async move {
        p_for_fetch
            .get_or_fetch(&1, move |id| {
                let go = go_for_fetch.clone();
                async move {
                    // Signal the fetcher body has been entered —
                    // Actor A is past the cache-miss path and
                    // registered in-flight before any Actor B work.
                    let _ = fetcher_started_tx.send(());
                    go.notified().await;
                    Ok::<_, FetchError>(Some(E {
                        id,
                        name: "actor_a".into(),
                    }))
                }
            })
            .await
    });

    // Wait for Actor A to enter the fetcher (deterministic — the
    // oneshot resolves only when the fetcher body runs, which only
    // happens after Actor A's initial get-miss + single-flight
    // registration).
    fetcher_started_rx
        .await
        .expect("Actor A's fetcher must enter before Actor B inserts");

    // Step 3: Actor B inserts at id=1 with default 1s TTL.
    p.insert(E {
        id: 1,
        name: "actor_b".into(),
    })
    .await
    .unwrap();

    // Step 4: advance past TTL. Actor B's entry is expired but still
    // resident; lazy reads do not physically clean it up.
    tokio::time::advance(Duration::from_secs(2)).await;

    // Step 5: wake Actor A's fetcher. on_fetched runs against an L1
    // with Actor B's expired entry.
    go.notify_one();

    let result = actor_a.await.unwrap().unwrap().unwrap();

    // Actor A always gets its value (this is true pre- and post-fix —
    // the bug was about L1 state, not return value).
    assert_eq!(result.name, "actor_a");

    // Headline assertion: subsequent get must hit. Pre-fix this would
    // miss (B stayed expired-resident, A was never inserted). Post-fix,
    // the atomic insert_arc_or_existing replaced B with A.
    let cached = p
        .get(&1)
        .expect("subsequent get must hit; pre-fix L1 was empty after expired-conflict");
    assert_eq!(cached.name, "actor_a");
}

#[tokio::test]
async fn single_flight_mismatched_fetch_id_broadcasts_error_to_all_waiters() {
    let p = Punnu::<E>::builder().build();
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..5 {
        let p = p.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            p.get_or_fetch(&1, move |_id| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok::<_, FetchError>(Some(E {
                        id: 2,
                        name: "wrong".into(),
                    }))
                }
            })
            .await
        }));
    }

    for h in handles {
        match h.await.unwrap() {
            Err(FetchError::IdentityMismatch { type_name }) => {
                assert!(
                    type_name.contains("E"),
                    "type_name should include the cached type's name; got {type_name}"
                );
            }
            other => panic!("expected IdentityMismatch on every peer, got {other:?}"),
        }
    }

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "fetcher should still coalesce to one origin call"
    );
    assert!(p.get(&1).is_none(), "requested id must not be cached");
    assert!(p.get(&2).is_none(), "returned wrong id must not be cached");
    assert_eq!(p.len(), 0);
}
