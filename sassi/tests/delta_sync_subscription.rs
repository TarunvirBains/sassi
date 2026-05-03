#![cfg(feature = "runtime-tokio")]

use sassi::{
    Cacheable, DeltaPunnuFetcher, DeltaQuery, DeltaResult, FetchError, MemQ, OnConflict, Punnu,
    PunnuConfig,
};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

#[derive(sassi::Cacheable, Clone, Debug, PartialEq, Eq)]
#[cacheable(watermark_field = "updated_at")]
struct Item {
    id: i64,
    updated_at: i64,
    group: &'static str,
    value: &'static str,
}

type RecordedQuery = (Option<i64>, HashSet<i64>);

#[derive(Clone)]
struct ScriptedDeltaFetcher {
    outcomes: Arc<Mutex<VecDeque<FetchOutcome>>>,
    queries: Arc<Mutex<Vec<RecordedQuery>>>,
    calls: Arc<AtomicUsize>,
}

enum FetchOutcome {
    Items(Vec<Item>),
    Delta(DeltaResult<Item, i64>),
    Err(&'static str),
    Panic(&'static str),
    Blocked {
        started: Arc<Notify>,
        release: Arc<Notify>,
        items: Vec<Item>,
    },
}

impl ScriptedDeltaFetcher {
    fn new(outcomes: impl Into<VecDeque<FetchOutcome>>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into())),
            queries: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    async fn queries(&self) -> Vec<RecordedQuery> {
        self.queries.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl DeltaPunnuFetcher<Item> for ScriptedDeltaFetcher {
    async fn fetch_delta(
        &self,
        query: DeltaQuery<Item>,
    ) -> Result<DeltaResult<Item, i64>, FetchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.queries
            .lock()
            .await
            .push((query.since, query.recover_ids));

        match self.outcomes.lock().await.pop_front() {
            Some(FetchOutcome::Items(items)) => Ok(DeltaResult::new(items, HashSet::new())),
            Some(FetchOutcome::Delta(delta)) => Ok(delta),
            Some(FetchOutcome::Err(message)) => Err(FetchError::Serialization(message.to_owned())),
            Some(FetchOutcome::Panic(message)) => panic!("{message}"),
            Some(FetchOutcome::Blocked {
                started,
                release,
                items,
            }) => {
                started.notify_one();
                release.notified().await;
                Ok(DeltaResult::new(items, HashSet::new()))
            }
            None => Ok(DeltaResult::new(Vec::new(), HashSet::new())),
        }
    }
}

fn item(id: i64, updated_at: i64, group: &'static str, value: &'static str) -> Item {
    Item {
        id,
        updated_at,
        group,
        value,
    }
}

fn long_interval() -> Duration {
    Duration::from_secs(3600)
}

async fn wait_for_notification(notify: &Notify, context: &'static str) {
    tokio::time::timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect(context);
}

#[tokio::test]
#[should_panic(expected = "delta refresh interval must be non-zero")]
async fn start_delta_refresh_rejects_zero_interval() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([]);

    let _ = punnu.start_delta_refresh(Duration::ZERO, fetcher);
}

#[test]
#[should_panic(expected = "Punnu::start_delta_refresh requires an active Tokio runtime")]
fn start_delta_refresh_requires_active_tokio_runtime() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([]);

    let _ = punnu.start_delta_refresh(long_interval(), fetcher);
}

#[test]
#[should_panic(expected = "delta refresh interval must be non-zero")]
fn start_delta_refresh_reports_zero_interval_before_missing_runtime() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([]);

    let _ = punnu.start_delta_refresh(Duration::ZERO, fetcher);
}

#[test]
#[should_panic(expected = "DeltaRefreshHandle::update requires an active Tokio runtime")]
fn update_requires_active_tokio_runtime_when_registering_work() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let handle = runtime.block_on(async {
        let punnu = Punnu::<Item>::builder().build();
        let fetcher = ScriptedDeltaFetcher::new([]);
        punnu.start_delta_refresh(long_interval(), fetcher)
    });

    futures::executor::block_on(handle.update()).unwrap();
}

#[test]
#[should_panic(expected = "DeltaRefreshHandle::update_full requires an active Tokio runtime")]
fn update_full_requires_active_tokio_runtime_when_registering_work() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let handle = runtime.block_on(async {
        let punnu = Punnu::<Item>::builder().build();
        let fetcher = ScriptedDeltaFetcher::new([]);
        punnu.start_delta_refresh(long_interval(), fetcher)
    });

    futures::executor::block_on(handle.update_full()).unwrap();
}

#[tokio::test]
async fn update_uses_previous_successful_high_watermark() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![item(1, 10, "a", "first")]),
        FetchOutcome::Items(vec![item(2, 13, "a", "second")]),
    ]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher.clone());

    let first = handle.update().await.unwrap();
    let second = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(first.applied, 1);
    assert_eq!(first.watermark, Some(10));
    assert_eq!(second.applied, 1);
    assert_eq!(second.watermark, Some(13));
    assert_eq!(punnu.get(&1).unwrap().value, "first");
    assert_eq!(punnu.get(&2).unwrap().value, "second");

    let queries = fetcher.queries().await;
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[0].0, None);
    assert!(queries[0].1.is_empty());
    assert_eq!(queries[1].0, Some(10));
    assert!(queries[1].1.is_empty());
}

#[tokio::test]
async fn update_full_uses_full_query_without_rolling_watermark_back() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![item(1, 100, "a", "newer")]),
        FetchOutcome::Items(vec![item(1, 50, "a", "older-full")]),
    ]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher.clone());

    handle.update().await.unwrap();
    let full = handle.update_full().await.unwrap();
    handle.cancel();

    assert_eq!(full.applied, 1);
    assert_eq!(full.watermark, Some(100));
    assert_eq!(handle.watermark(), Some(100));
    assert_eq!(punnu.get(&1).unwrap().value, "older-full");

    let queries = fetcher.queries().await;
    assert_eq!(queries[0].0, None);
    assert_eq!(queries[1].0, None);
}

#[tokio::test]
async fn boundary_watermark_item_is_reapplied_and_deduped_by_identity() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![item(1, 10, "a", "boundary-old")]),
        FetchOutcome::Items(vec![
            item(1, 10, "a", "boundary-new"),
            item(2, 10, "a", "same-boundary"),
        ]),
    ]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher.clone());

    handle.update().await.unwrap();
    let second = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(second.applied, 2);
    assert_eq!(second.watermark, Some(10));
    assert_eq!(punnu.get(&1).unwrap().value, "boundary-new");
    assert_eq!(punnu.get(&2).unwrap().value, "same-boundary");
    assert_eq!(fetcher.queries().await[1].0, Some(10));
}

#[tokio::test]
async fn two_subscriptions_over_same_punnu_keep_independent_watermarks() {
    let punnu = Punnu::<Item>::builder().build();
    let active = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![item(1, 100, "active", "active-1")]),
        FetchOutcome::Items(vec![item(2, 101, "active", "active-2")]),
    ]);
    let tenant = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![item(10, 5, "tenant", "tenant-1")]),
        FetchOutcome::Items(vec![item(11, 6, "tenant", "tenant-2")]),
    ]);
    let active_handle = punnu.start_delta_refresh(long_interval(), active.clone());
    let tenant_handle = punnu.start_delta_refresh(long_interval(), tenant.clone());

    active_handle.update().await.unwrap();
    tenant_handle.update().await.unwrap();
    active_handle.update().await.unwrap();
    tenant_handle.update().await.unwrap();
    active_handle.cancel();
    tenant_handle.cancel();

    assert_eq!(active.queries().await[1].0, Some(100));
    assert_eq!(tenant.queries().await[1].0, Some(5));
    assert_eq!(punnu.get(&1).unwrap().value, "active-1");
    assert_eq!(punnu.get(&2).unwrap().value, "active-2");
    assert_eq!(punnu.get(&10).unwrap().value, "tenant-1");
    assert_eq!(punnu.get(&11).unwrap().value, "tenant-2");
}

#[tokio::test]
async fn inflight_slot_is_scoped_per_subscription() {
    let punnu = Punnu::<Item>::builder().build();
    let a_started = Arc::new(Notify::new());
    let a_release = Arc::new(Notify::new());
    let b_started = Arc::new(Notify::new());
    let b_release = Arc::new(Notify::new());
    let fetcher_a = ScriptedDeltaFetcher::new([FetchOutcome::Blocked {
        started: a_started.clone(),
        release: a_release.clone(),
        items: vec![item(1, 10, "a", "a")],
    }]);
    let fetcher_b = ScriptedDeltaFetcher::new([FetchOutcome::Blocked {
        started: b_started.clone(),
        release: b_release.clone(),
        items: vec![item(2, 20, "b", "b")],
    }]);
    let handle_a = Arc::new(punnu.start_delta_refresh(long_interval(), fetcher_a.clone()));
    let handle_b = Arc::new(punnu.start_delta_refresh(long_interval(), fetcher_b.clone()));

    let a1 = tokio::spawn({
        let handle = handle_a.clone();
        async move { handle.update().await }
    });
    wait_for_notification(&a_started, "subscription A delta should start").await;
    let a2 = tokio::spawn({
        let handle = handle_a.clone();
        async move { handle.update().await }
    });
    let b1 = tokio::spawn({
        let handle = handle_b.clone();
        async move { handle.update().await }
    });
    wait_for_notification(&b_started, "subscription B delta should start").await;

    assert_eq!(fetcher_a.calls(), 1);
    assert_eq!(fetcher_b.calls(), 1);

    a_release.notify_one();
    b_release.notify_one();
    a1.await.unwrap().unwrap();
    a2.await.unwrap().unwrap();
    b1.await.unwrap().unwrap();
    handle_a.cancel();
    handle_b.cancel();

    assert_eq!(fetcher_a.calls(), 1);
    assert_eq!(fetcher_b.calls(), 1);
    assert_eq!(punnu.get(&1).unwrap().value, "a");
    assert_eq!(punnu.get(&2).unwrap().value, "b");
}

#[tokio::test]
async fn five_concurrent_updates_share_one_delta_fetch() {
    let punnu = Punnu::<Item>::builder().build();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let fetcher = ScriptedDeltaFetcher::new([FetchOutcome::Blocked {
        started: started.clone(),
        release: release.clone(),
        items: vec![item(1, 10, "a", "coalesced")],
    }]);
    let handle = Arc::new(punnu.start_delta_refresh(long_interval(), fetcher.clone()));

    let tasks = (0..5)
        .map(|_| {
            let handle = handle.clone();
            tokio::spawn(async move { handle.update().await })
        })
        .collect::<Vec<_>>();
    wait_for_notification(&started, "coalesced delta should start").await;
    assert_eq!(fetcher.calls(), 1);

    release.notify_one();
    for task in tasks {
        let result = task.await.unwrap().unwrap();
        assert_eq!(result.watermark, Some(10));
    }
    handle.cancel();

    assert_eq!(fetcher.calls(), 1);
    assert_eq!(punnu.get(&1).unwrap().value, "coalesced");
}

#[tokio::test]
async fn update_full_waiting_on_delta_cannot_be_starved_by_more_updates() {
    let punnu = Punnu::<Item>::builder().build();
    let delta_started = Arc::new(Notify::new());
    let delta_release = Arc::new(Notify::new());
    let full_started = Arc::new(Notify::new());
    let full_release = Arc::new(Notify::new());
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Blocked {
            started: delta_started.clone(),
            release: delta_release.clone(),
            items: vec![item(1, 10, "delta", "delta")],
        },
        FetchOutcome::Blocked {
            started: full_started.clone(),
            release: full_release.clone(),
            items: vec![item(2, 20, "full", "full")],
        },
    ]);
    let handle = Arc::new(punnu.start_delta_refresh(long_interval(), fetcher.clone()));

    let delta = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update().await }
    });
    wait_for_notification(&delta_started, "starvation test delta should start").await;
    let full_handle = handle.clone();
    let full = full_handle.update_full();
    tokio::pin!(full);
    assert!(
        futures::poll!(&mut full).is_pending(),
        "update_full should attach to the in-flight delta before noisy updates arrive"
    );
    let mut noisy_deltas = (0..20)
        .map(|_| Box::pin(handle.update()))
        .collect::<Vec<_>>();
    for future in &mut noisy_deltas {
        assert!(
            futures::poll!(future.as_mut()).is_pending(),
            "noisy delta should attach to the existing in-flight delta"
        );
    }

    assert_eq!(fetcher.calls(), 1);
    delta_release.notify_one();
    wait_for_notification(&full_started, "starvation test full should start").await;
    assert_eq!(fetcher.calls(), 2);

    full_release.notify_one();

    assert_eq!(delta.await.unwrap().unwrap().watermark, Some(10));
    assert_eq!(full.await.unwrap().watermark, Some(20));
    for future in noisy_deltas {
        future.await.unwrap();
    }
    handle.cancel();

    assert_eq!(fetcher.calls(), 2);
    assert_eq!(punnu.get(&1).unwrap().value, "delta");
    assert_eq!(punnu.get(&2).unwrap().value, "full");
}

#[tokio::test]
async fn dropped_update_full_future_still_runs_registered_full_refresh() {
    let punnu = Punnu::<Item>::builder().build();
    let delta_started = Arc::new(Notify::new());
    let delta_release = Arc::new(Notify::new());
    let full_started = Arc::new(Notify::new());
    let full_release = Arc::new(Notify::new());
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Blocked {
            started: delta_started.clone(),
            release: delta_release.clone(),
            items: vec![item(1, 10, "delta", "delta")],
        },
        FetchOutcome::Blocked {
            started: full_started.clone(),
            release: full_release.clone(),
            items: vec![item(2, 20, "full", "full")],
        },
    ]);
    let handle = Arc::new(punnu.start_delta_refresh(long_interval(), fetcher.clone()));

    let delta = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update().await }
    });
    wait_for_notification(
        &delta_started,
        "dropped update_full test delta should start",
    )
    .await;
    let full_handle = handle.clone();
    let mut full = Box::pin(full_handle.update_full());
    assert!(
        futures::poll!(full.as_mut()).is_pending(),
        "polling update_full once registers the chained full refresh"
    );
    drop(full);

    delta_release.notify_one();
    wait_for_notification(&full_started, "dropped update_full test full should start").await;
    assert_eq!(fetcher.calls(), 2);
    full_release.notify_one();

    assert_eq!(delta.await.unwrap().unwrap().watermark, Some(10));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if punnu.get(&2).is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("registered full refresh should commit even after caller drops its future");
    handle.cancel();

    assert_eq!(punnu.get(&2).unwrap().value, "full");
}

#[tokio::test]
async fn update_full_joiners_share_chained_full_after_delta_finishes_quickly() {
    let punnu = Punnu::<Item>::builder().build();
    let delta_started = Arc::new(Notify::new());
    let delta_release = Arc::new(Notify::new());
    let full_started = Arc::new(Notify::new());
    let full_release = Arc::new(Notify::new());
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Blocked {
            started: delta_started.clone(),
            release: delta_release.clone(),
            items: vec![item(1, 10, "delta", "delta")],
        },
        FetchOutcome::Blocked {
            started: full_started.clone(),
            release: full_release.clone(),
            items: vec![item(2, 20, "full", "full")],
        },
        FetchOutcome::Items(vec![item(3, 30, "unexpected", "second-full")]),
    ]);
    let handle = Arc::new(punnu.start_delta_refresh(long_interval(), fetcher.clone()));

    let delta = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update().await }
    });
    wait_for_notification(&delta_started, "fast-full test delta should start").await;
    let first_full = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update_full().await }
    });
    tokio::task::yield_now().await;
    delta_release.notify_one();
    wait_for_notification(&full_started, "fast-full test full should start").await;
    let second_full = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update_full().await }
    });
    let delta_after_full_started = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update().await }
    });
    tokio::task::yield_now().await;

    assert_eq!(
        fetcher.calls(),
        2,
        "update_full joiner must attach to the already chained full instead of spawning another"
    );
    full_release.notify_one();

    assert_eq!(delta.await.unwrap().unwrap().watermark, Some(10));
    assert_eq!(first_full.await.unwrap().unwrap().watermark, Some(20));
    assert_eq!(second_full.await.unwrap().unwrap().watermark, Some(20));
    assert_eq!(
        delta_after_full_started.await.unwrap().unwrap().watermark,
        Some(20),
        "delta update after Delta -> Full transition should follow the in-flight full"
    );
    handle.cancel();

    assert_eq!(fetcher.calls(), 2);
    assert!(punnu.get(&3).is_none());
}

#[tokio::test]
async fn dropping_update_future_does_not_cancel_spawned_fetch_task() {
    let punnu = Punnu::<Item>::builder().build();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let fetcher = ScriptedDeltaFetcher::new([FetchOutcome::Blocked {
        started: started.clone(),
        release: release.clone(),
        items: vec![item(1, 10, "a", "committed-after-drop")],
    }]);
    let handle = Arc::new(punnu.start_delta_refresh(long_interval(), fetcher.clone()));

    let caller = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update().await }
    });
    wait_for_notification(&started, "detached delta fetch should start").await;
    caller.abort();

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if punnu.get(&1).is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached delta fetch should commit after caller drop");
    handle.cancel();

    assert_eq!(fetcher.calls(), 1);
    assert_eq!(punnu.get(&1).unwrap().value, "committed-after-drop");
}

#[tokio::test]
async fn fetch_failure_does_not_advance_watermark_or_mutate_l1() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![item(1, 10, "a", "initial")]),
        FetchOutcome::Err("source unavailable"),
        FetchOutcome::Items(vec![item(2, 11, "a", "after-retry")]),
    ]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher.clone());

    handle.update().await.unwrap();
    let err = handle.update().await.unwrap_err();
    assert!(matches!(err, FetchError::Serialization(_)));
    assert_eq!(handle.watermark(), Some(10));
    assert_eq!(punnu.len(), 1);
    assert_eq!(punnu.get(&1).unwrap().value, "initial");
    assert!(punnu.get(&2).is_none());

    let retry = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(retry.watermark, Some(11));
    assert_eq!(punnu.get(&2).unwrap().value, "after-retry");
    let queries = fetcher.queries().await;
    assert_eq!(queries[1].0, Some(10));
    assert_eq!(queries[2].0, Some(10));
}

#[tokio::test]
async fn fetcher_panic_does_not_advance_watermark_or_mutate_l1() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![item(1, 10, "a", "initial")]),
        FetchOutcome::Panic("delta source panicked"),
        FetchOutcome::Items(vec![item(2, 11, "a", "after-panic")]),
    ]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher.clone());

    handle.update().await.unwrap();
    let err = handle.update().await.unwrap_err();
    assert!(matches!(
        err,
        FetchError::FetcherPanic {
            message,
            ..
        } if message == "delta source panicked"
    ));
    assert_eq!(handle.watermark(), Some(10));
    assert_eq!(punnu.len(), 1);
    assert_eq!(punnu.get(&1).unwrap().value, "initial");
    assert!(punnu.get(&2).is_none());

    let retry = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(retry.watermark, Some(11));
    assert_eq!(punnu.get(&2).unwrap().value, "after-panic");
    let queries = fetcher.queries().await;
    assert_eq!(queries[1].0, Some(10));
    assert_eq!(queries[2].0, Some(10));
}

#[tokio::test]
async fn rejected_delta_items_still_advance_the_source_watermark() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Reject,
            ..Default::default()
        })
        .build();
    punnu
        .insert(item(1, 10, "a", "resident"))
        .await
        .expect("seed resident item");
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![item(1, 100, "a", "rejected")]),
        FetchOutcome::Items(vec![item(2, 101, "a", "after-retry")]),
    ]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher.clone());

    let rejected = handle.update().await.unwrap();
    let retried = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(rejected.applied, 0);
    assert_eq!(
        rejected.watermark,
        Some(100),
        "the stream cursor tracks processed source rows, not cache retention"
    );
    assert_eq!(punnu.get(&1).unwrap().value, "resident");
    assert_eq!(
        retried.watermark,
        Some(101),
        "next update should continue from the processed source boundary"
    );
    let queries = fetcher.queries().await;
    assert_eq!(queries[0].0, None);
    assert_eq!(queries[1].0, Some(100));
}

#[tokio::test]
async fn mixed_rejected_delta_batch_advances_to_the_processed_batch_watermark() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Reject,
            ..Default::default()
        })
        .build();
    punnu
        .insert(item(1, 1, "a", "resident"))
        .await
        .expect("seed resident item");
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![
            item(1, 50, "a", "rejected-low"),
            item(2, 100, "a", "accepted-high"),
        ]),
        FetchOutcome::Items(vec![item(3, 101, "a", "after-retry")]),
    ]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher.clone());

    let mixed = handle.update().await.unwrap();
    let retry = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(mixed.applied, 1);
    assert_eq!(
        mixed.watermark,
        Some(100),
        "the cursor advances to the processed batch watermark even when conflict policy rejects one row"
    );
    assert_eq!(punnu.get(&1).unwrap().value, "resident");
    assert_eq!(punnu.get(&2).unwrap().value, "accepted-high");
    assert_eq!(retry.watermark, Some(101));
    let queries = fetcher.queries().await;
    assert_eq!(queries[0].0, None);
    assert_eq!(queries[1].0, Some(100));
}

#[tokio::test]
async fn lru_evicted_delta_item_still_advances_to_observed_high_watermark() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let fetcher =
        ScriptedDeltaFetcher::new([FetchOutcome::Delta(DeltaResult::with_high_watermark(
            vec![item(1, 10, "a", "lower"), item(2, 20, "a", "higher")],
            HashSet::new(),
            200,
        ))]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher);

    let result = handle.update().await.unwrap();
    handle.cancel();

    let id_one_resident = punnu.get(&1).is_some();
    let id_two_resident = punnu.get(&2).is_some();
    assert_ne!(
        id_one_resident, id_two_resident,
        "capacity-one delta should publish exactly one fetched item"
    );
    let evicted_watermark = if id_one_resident { 20 } else { 10 };
    assert_eq!(
        result.watermark,
        Some(200),
        "source progress must not be pinned to rows that happen to remain in L1"
    );
    assert!(
        evicted_watermark == 10 || evicted_watermark == 20,
        "test sanity check: one fetched row should have been evicted"
    );
}

#[tokio::test]
async fn high_watermark_lower_than_item_watermark_advances_to_item_maximum() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([FetchOutcome::Delta(
        DeltaResult::with_high_watermark(vec![item(1, 30, "a", "newer-item")], HashSet::new(), 20),
    )]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher);

    let result = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(result.watermark, Some(30));
}

#[tokio::test]
async fn lru_evicted_delta_item_without_explicit_high_watermark_uses_item_maximum() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let fetcher = ScriptedDeltaFetcher::new([FetchOutcome::Items(vec![
        item(1, 10, "a", "lower"),
        item(2, 20, "a", "higher"),
    ])]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher);

    let result = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(result.watermark, Some(20));
    assert_eq!(punnu.len(), 1);
}

#[tokio::test]
async fn tombstone_only_high_watermark_advances_the_subscription_cursor() {
    let punnu = Punnu::<Item>::builder().build();
    punnu
        .insert(item(1, 10, "a", "resident"))
        .await
        .expect("seed resident item");
    let fetcher = ScriptedDeltaFetcher::new([FetchOutcome::Delta(
        DeltaResult::with_high_watermark(Vec::new(), HashSet::from([1]), 30),
    )]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher);

    let result = handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(result.applied, 0);
    assert_eq!(result.watermark, Some(30));
    assert!(punnu.get(&1).is_none());
}

#[tokio::test]
async fn delta_can_update_row_that_leaves_a_cached_query_filter_without_deleting_it() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedDeltaFetcher::new([
        FetchOutcome::Items(vec![item(1, 10, "active", "before")]),
        FetchOutcome::Items(vec![item(1, 11, "archived", "after")]),
    ]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher);

    handle.update().await.unwrap();
    let active_before = punnu
        .scope(vec![MemQ::filter_basic(Item::fields().group.eq("active"))])
        .collect();
    assert_eq!(active_before.len(), 1);

    handle.update().await.unwrap();
    handle.cancel();

    assert!(
        punnu.get(&1).is_some(),
        "filter departure is not a tombstone"
    );
    assert_eq!(punnu.get(&1).unwrap().group, "archived");
    assert!(
        punnu
            .scope(vec![MemQ::filter_basic(Item::fields().group.eq("active"))])
            .collect()
            .is_empty(),
        "read-time query filtering must stop showing rows whose cached value no longer matches"
    );
}
