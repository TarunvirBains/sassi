#![cfg(feature = "runtime-tokio")]

use sassi::{DeltaPunnuFetcher, DeltaQuery, DeltaResult, FetchError, Punnu, PunnuConfig};
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
    value: &'static str,
}

type RecordedQuery = (Option<i64>, HashSet<i64>);

#[derive(Clone)]
struct RecordingFetcher {
    outcomes: Arc<Mutex<VecDeque<FetchOutcome>>>,
    queries: Arc<Mutex<Vec<RecordedQuery>>>,
    calls: Arc<AtomicUsize>,
}

enum FetchOutcome {
    Delta(DeltaResult<Item, i64>),
    Err(&'static str),
    BlockedDelta {
        started: Arc<Notify>,
        release: Arc<Notify>,
        delta: DeltaResult<Item, i64>,
    },
    BlockedErr {
        started: Arc<Notify>,
        release: Arc<Notify>,
    },
}

impl RecordingFetcher {
    fn new(outcomes: impl Into<VecDeque<DeltaResult<Item, i64>>>) -> Self {
        Self::scripted(
            outcomes
                .into()
                .into_iter()
                .map(FetchOutcome::Delta)
                .collect::<VecDeque<_>>(),
        )
    }

    fn scripted(outcomes: impl Into<VecDeque<FetchOutcome>>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into())),
            queries: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn queries(&self) -> Vec<RecordedQuery> {
        self.queries.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl DeltaPunnuFetcher<Item> for RecordingFetcher {
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
            Some(FetchOutcome::Delta(delta)) => Ok(delta),
            Some(FetchOutcome::Err(message)) => Err(FetchError::Serialization(message.to_owned())),
            Some(FetchOutcome::BlockedDelta {
                started,
                release,
                delta,
            }) => {
                started.notify_one();
                release.notified().await;
                Ok(delta)
            }
            Some(FetchOutcome::BlockedErr { started, release }) => {
                started.notify_one();
                release.notified().await;
                Err(FetchError::Serialization("blocked failure".to_owned()))
            }
            None => Ok(DeltaResult::new(Vec::new(), HashSet::new())),
        }
    }
}

fn item(id: i64, updated_at: i64, value: &'static str) -> Item {
    Item {
        id,
        updated_at,
        value,
    }
}

fn delta(items: Vec<Item>) -> DeltaResult<Item, i64> {
    DeltaResult::new(items, HashSet::new())
}

fn long_interval() -> Duration {
    Duration::from_secs(3600)
}

async fn wait_until(mut predicate: impl FnMut() -> bool, context: &'static str) {
    tokio::time::timeout(Duration::from_secs(2), async move {
        loop {
            if predicate() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(context);
}

async fn wait_for_notification(notify: &Notify, context: &'static str) {
    tokio::time::timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect(context);
}

#[tokio::test]
async fn with_eviction_recovery_adds_evicted_ids_to_next_delta_query() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let fetcher = RecordingFetcher::new([
        delta(vec![item(1, 10, "initial")]),
        delta(vec![item(1, 11, "recovered")]),
    ]);
    let handle = punnu
        .start_delta_refresh(long_interval(), fetcher.clone())
        .with_eviction_recovery(true);

    handle.update().await.unwrap();
    punnu.insert(item(2, 20, "evictor")).await.unwrap();
    wait_until(
        || handle.pending_eviction_recovery_count() == 1,
        "LRU eviction should be queued for recovery",
    )
    .await;

    handle.update().await.unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[1].0, Some(10));
    assert_eq!(queries[1].1, HashSet::from([1]));
    assert_eq!(punnu.get(&1).unwrap().value, "recovered");
}

#[tokio::test]
async fn recovery_ids_are_scoped_per_subscription() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let fetcher_a = RecordingFetcher::new([
        delta(vec![item(1, 10, "a-initial")]),
        delta(vec![item(1, 12, "a-recovered")]),
    ]);
    let fetcher_b = RecordingFetcher::new([
        delta(vec![item(2, 20, "b-initial")]),
        DeltaResult::new(Vec::new(), HashSet::new()),
    ]);
    let handle_a = punnu
        .start_delta_refresh(long_interval(), fetcher_a.clone())
        .with_eviction_recovery(true);
    let handle_b = punnu
        .start_delta_refresh(long_interval(), fetcher_b.clone())
        .with_eviction_recovery(true);

    handle_a.update().await.unwrap();
    handle_b.update().await.unwrap();
    wait_until(
        || handle_a.pending_eviction_recovery_count() == 1,
        "subscription A should record eviction of its own id",
    )
    .await;

    handle_b.update().await.unwrap();
    handle_a.update().await.unwrap();
    handle_a.cancel();
    handle_b.cancel();

    let a_queries = fetcher_a.queries().await;
    let b_queries = fetcher_b.queries().await;
    assert_eq!(a_queries[1].1, HashSet::from([1]));
    assert!(b_queries[1].1.is_empty());
}

#[tokio::test]
async fn enabling_eviction_recovery_after_eviction_preserves_observed_ids() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let fetcher = RecordingFetcher::new([
        delta(vec![item(1, 10, "initial")]),
        delta(vec![item(1, 11, "recovered")]),
    ]);
    let handle = punnu.start_delta_refresh(long_interval(), fetcher.clone());

    handle.update().await.unwrap();
    punnu.insert(item(2, 20, "evictor")).await.unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(handle.pending_eviction_recovery_count(), 0);

    let handle = handle.with_eviction_recovery(true);
    wait_until(
        || handle.pending_eviction_recovery_count() == 1,
        "pre-enable LRU eviction should be retained for recovery",
    )
    .await;

    handle.update().await.unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries[1].1, HashSet::from([1]));
}

#[tokio::test]
async fn event_lag_forces_full_refresh_instead_of_partial_recovery() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            event_channel_capacity: 1,
            ..Default::default()
        })
        .build();
    let fetcher = RecordingFetcher::new([
        delta(vec![item(1, 10, "initial")]),
        delta(vec![item(1, 11, "full-after-lag")]),
    ]);
    let handle = punnu
        .start_delta_refresh(long_interval(), fetcher.clone())
        .with_eviction_recovery(true);

    handle.update().await.unwrap();
    for id in 2..100 {
        punnu
            .insert(item(id, 20 + id, "evictor"))
            .await
            .expect("burst insert should succeed");
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    handle.update().await.unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries[1].0, None);
    assert!(queries[1].1.is_empty());
}

#[tokio::test]
async fn failed_lag_forced_full_refresh_keeps_forcing_full() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            event_channel_capacity: 1,
            ..Default::default()
        })
        .build();
    let fetcher = RecordingFetcher::scripted([
        FetchOutcome::Delta(delta(vec![item(1, 10, "initial")])),
        FetchOutcome::Err("forced full failed"),
        FetchOutcome::Delta(delta(vec![item(1, 12, "full-after-retry")])),
    ]);
    let handle = punnu
        .start_delta_refresh(long_interval(), fetcher.clone())
        .with_eviction_recovery(true);

    handle.update().await.unwrap();
    for id in 2..100 {
        punnu
            .insert(item(id, 20 + id, "evictor"))
            .await
            .expect("burst insert should succeed");
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    assert!(matches!(
        handle.update().await.unwrap_err(),
        FetchError::Serialization(_)
    ));
    handle.update().await.unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries[1].0, None);
    assert_eq!(queries[2].0, None);
}

#[tokio::test]
async fn lag_during_inflight_full_requires_another_full_refresh() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            event_channel_capacity: 1,
            ..Default::default()
        })
        .build();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let fetcher = RecordingFetcher::scripted([
        FetchOutcome::Delta(delta(vec![item(1, 10, "initial")])),
        FetchOutcome::BlockedDelta {
            started: started.clone(),
            release: release.clone(),
            delta: delta(vec![item(2, 20, "manual-full")]),
        },
        FetchOutcome::Delta(delta(vec![item(3, 30, "lag-repair-full")])),
    ]);
    let handle = Arc::new(
        punnu
            .start_delta_refresh(long_interval(), fetcher.clone())
            .with_eviction_recovery(true),
    );

    handle.update().await.unwrap();
    let full = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update_full().await }
    });
    wait_for_notification(&started, "manual full should start").await;
    for id in 4..100 {
        punnu
            .insert(item(id, 40 + id, "evictor"))
            .await
            .expect("burst insert should succeed");
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    release.notify_one();
    full.await.unwrap().unwrap();
    handle.update().await.unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries[1].0, None);
    assert_eq!(queries[2].0, None);
}

#[tokio::test]
async fn same_batch_lru_eviction_is_queued_for_recovery() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let fetcher = RecordingFetcher::new([
        delta(vec![item(1, 10, "one"), item(2, 20, "two")]),
        delta(vec![item(1, 30, "recovered")]),
    ]);
    let handle = punnu
        .start_delta_refresh(long_interval(), fetcher.clone())
        .with_eviction_recovery(true);

    handle.update().await.unwrap();
    wait_until(
        || handle.pending_eviction_recovery_count() == 1,
        "same-batch LRU eviction should be queued for recovery",
    )
    .await;

    handle.update().await.unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries[1].1.len(), 1);
}

#[tokio::test]
async fn recovery_overflow_forces_full_refresh() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let fetcher = RecordingFetcher::new([
        delta(vec![item(1, 10, "one")]),
        delta(vec![item(2, 20, "two")]),
        delta(vec![item(1, 30, "full")]),
    ]);
    let handle = punnu
        .start_delta_refresh(long_interval(), fetcher.clone())
        .with_eviction_recovery(true);

    handle.update().await.unwrap();
    handle.update().await.unwrap();
    wait_until(
        || handle.pending_eviction_recovery_count() == 1,
        "first LRU eviction should be pending",
    )
    .await;
    punnu.insert(item(3, 30, "three")).await.unwrap();
    wait_until(
        || handle.pending_eviction_recovery_count() == 2,
        "second LRU eviction should overflow the capacity-one recovery set",
    )
    .await;

    handle.update().await.unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries[2].0, None);
    assert!(queries[2].1.is_empty());
}

#[tokio::test]
async fn failed_full_refresh_restores_snapshot_without_losing_new_evictions() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let fetcher = RecordingFetcher::scripted([
        FetchOutcome::Delta(delta(vec![item(1, 10, "one")])),
        FetchOutcome::Delta(delta(vec![item(2, 20, "two")])),
        FetchOutcome::Delta(delta(vec![item(3, 30, "three")])),
        FetchOutcome::BlockedErr {
            started: started.clone(),
            release: release.clone(),
        },
    ]);
    let handle = Arc::new(
        punnu
            .start_delta_refresh(long_interval(), fetcher)
            .with_eviction_recovery(true),
    );

    handle.update().await.unwrap();
    handle.update().await.unwrap();
    handle.update().await.unwrap();
    wait_until(
        || handle.pending_eviction_recovery_count() == 2,
        "two evictions should overflow recovery before full refresh",
    )
    .await;

    let full = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update().await }
    });
    wait_for_notification(&started, "overflow full refresh should start").await;
    punnu.insert(item(4, 40, "four")).await.unwrap();
    release.notify_one();
    let err = full.await.unwrap().unwrap_err();
    assert!(matches!(err, FetchError::Serialization(_)));
    wait_until(
        || handle.pending_eviction_recovery_count() == 3,
        "failed full refresh should restore its snapshot and preserve in-flight eviction",
    )
    .await;
    handle.cancel();
}

#[tokio::test]
async fn periodic_full_refresh_runs_on_nth_tick_and_manual_full_resets_progress() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = RecordingFetcher::new([
        delta(vec![item(1, 10, "tick-1")]),
        delta(vec![item(2, 20, "tick-2")]),
        delta(vec![item(3, 30, "tick-3-full")]),
        delta(vec![item(4, 40, "manual-full")]),
    ]);
    let handle = punnu
        .start_delta_refresh(Duration::from_millis(10), fetcher.clone())
        .with_periodic_full_refresh(Some(std::num::NonZeroUsize::new(3).unwrap()));

    wait_until(|| fetcher.calls.load(Ordering::SeqCst) >= 1, "tick 1").await;
    wait_until(|| fetcher.calls.load(Ordering::SeqCst) >= 2, "tick 2").await;
    wait_until(|| fetcher.calls.load(Ordering::SeqCst) >= 3, "tick 3").await;

    handle.update_full().await.unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries[0].0, None);
    assert_eq!(queries[1].0, Some(10));
    assert_eq!(queries[2].0, None);
    assert_eq!(
        handle
            .periodic_full_refresh_progress()
            .map(|(elapsed, every)| (elapsed, every.get())),
        Some((0, 3))
    );
}

#[tokio::test]
async fn manual_delta_update_does_not_advance_periodic_full_progress() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = RecordingFetcher::new([
        delta(vec![item(1, 10, "manual-1")]),
        delta(vec![item(2, 20, "manual-2")]),
    ]);
    let handle = punnu
        .start_delta_refresh(long_interval(), fetcher)
        .with_periodic_full_refresh(Some(std::num::NonZeroUsize::new(3).unwrap()));

    handle.update().await.unwrap();
    handle.update().await.unwrap();
    handle.cancel();

    assert_eq!(
        handle
            .periodic_full_refresh_progress()
            .map(|(elapsed, every)| (elapsed, every.get())),
        Some((0, 3))
    );
}

#[tokio::test]
async fn recovery_set_merges_back_on_fetch_error() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let fetcher = RecordingFetcher::scripted([
        FetchOutcome::Delta(delta(vec![item(1, 10, "seed")])),
        FetchOutcome::Err("recovery error"),
        FetchOutcome::Delta(delta(vec![item(1, 20, "recovered")])),
    ]);
    let handle = punnu
        .start_delta_refresh(long_interval(), fetcher.clone())
        .with_eviction_recovery(true);

    handle.update().await.unwrap();
    punnu.insert(item(2, 20, "evictor")).await.unwrap();
    wait_until(
        || handle.pending_eviction_recovery_count() == 1,
        "ID should be queued for recovery",
    )
    .await;

    let err = handle.update().await.unwrap_err();
    assert!(matches!(err, FetchError::Serialization(_)));

    handle.update().await.unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries.len(), 3);
    assert_eq!(queries[1].1, HashSet::from([1]));
    assert_eq!(queries[2].1, HashSet::from([1]));
    assert_eq!(punnu.get(&1).unwrap().value, "recovered");
}

#[tokio::test]
async fn stale_recovery_respects_concurrent_insert() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            lru_size: 1,
            ..Default::default()
        })
        .build();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let fetcher = RecordingFetcher::scripted([
        FetchOutcome::Delta(delta(vec![item(1, 10, "seed")])),
        FetchOutcome::BlockedDelta {
            started: started.clone(),
            release: release.clone(),
            delta: delta(vec![item(1, 11, "older-recovered")]),
        },
    ]);
    let handle = Arc::new(
        punnu
            .start_delta_refresh(long_interval(), fetcher.clone())
            .with_eviction_recovery(true),
    );

    handle.update().await.unwrap();
    punnu.insert(item(2, 20, "evictor")).await.unwrap();
    wait_until(
        || handle.pending_eviction_recovery_count() == 1,
        "ID should be queued for recovery",
    )
    .await;

    let recovery = tokio::spawn({
        let handle = handle.clone();
        async move { handle.update().await }
    });
    wait_for_notification(&started, "stale recovery fetch should start").await;
    punnu.insert(item(1, 30, "newer-manual")).await.unwrap();
    release.notify_one();

    recovery
        .await
        .expect("recovery task should complete")
        .unwrap();
    handle.cancel();

    let queries = fetcher.queries().await;
    assert_eq!(queries[1].1, HashSet::from([1]));
    assert_eq!(punnu.get(&1).unwrap().value, "newer-manual");
    assert!(punnu.get(&2).is_none());
}
