#![cfg(feature = "runtime-tokio")]

use sassi::{
    BackendFailureMode, Cacheable, FetchError, Field, InvalidationReason, OnConflict, Punnu,
    PunnuConfig, PunnuFetcher, RefreshMode,
};
use std::collections::VecDeque;
use std::future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Item {
    id: i64,
    query: &'static str,
    value: &'static str,
}

#[derive(Default)]
struct ItemFields {
    #[allow(dead_code)]
    id: Field<Item, i64>,
    #[allow(dead_code)]
    query: Field<Item, &'static str>,
    #[allow(dead_code)]
    value: Field<Item, &'static str>,
}

impl Cacheable for Item {
    type Id = i64;
    type Fields = ItemFields;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {
        ItemFields {
            id: Field::new("id", |item| &item.id),
            query: Field::new("query", |item| &item.query),
            value: Field::new("value", |item| &item.value),
        }
    }
}

#[derive(Clone)]
struct ScriptedFetcher {
    outcomes: Arc<Mutex<VecDeque<FetchOutcome>>>,
    calls: Arc<AtomicUsize>,
}

enum FetchOutcome {
    Items(Vec<Item>),
    Err(&'static str),
}

impl ScriptedFetcher {
    fn new(outcomes: impl Into<VecDeque<FetchOutcome>>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl PunnuFetcher<Item> for ScriptedFetcher {
    async fn fetch(&self) -> Result<Vec<Item>, FetchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.outcomes.lock().await.pop_front() {
            Some(FetchOutcome::Items(items)) => Ok(items),
            Some(FetchOutcome::Err(message)) => Err(FetchError::Serialization(message.to_owned())),
            None => Ok(Vec::new()),
        }
    }
}

#[derive(Clone)]
struct BlockingFetcher {
    started: Arc<Notify>,
    dropped: Arc<Notify>,
}

impl BlockingFetcher {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            dropped: Arc::new(Notify::new()),
        }
    }
}

struct DropNotifier(Arc<Notify>);

impl Drop for DropNotifier {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

#[async_trait::async_trait]
impl PunnuFetcher<Item> for BlockingFetcher {
    async fn fetch(&self) -> Result<Vec<Item>, FetchError> {
        let _drop = DropNotifier(self.dropped.clone());
        self.started.notify_waiters();
        future::pending::<()>().await;
        unreachable!("pending fetch should only finish by being dropped")
    }
}

#[tokio::test]
async fn refresh_now_upsert_only_preserves_unrelated_query_entries() {
    let punnu = Punnu::<Item>::builder().build();
    punnu
        .insert(Item {
            id: 1,
            query: "query-a",
            value: "old-a",
        })
        .await
        .unwrap();
    punnu
        .insert(Item {
            id: 99,
            query: "query-b",
            value: "keep-b",
        })
        .await
        .unwrap();

    let fetcher = ScriptedFetcher::new([FetchOutcome::Items(vec![
        Item {
            id: 1,
            query: "query-a",
            value: "new-a",
        },
        Item {
            id: 2,
            query: "query-a",
            value: "new-a2",
        },
    ])]);
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        fetcher.clone(),
        RefreshMode::UpsertOnly,
    );

    handle.refresh_now().await.unwrap();
    handle.cancel();

    assert_eq!(punnu.get(&1).unwrap().value, "new-a");
    assert_eq!(punnu.get(&2).unwrap().value, "new-a2");
    assert_eq!(punnu.get(&99).unwrap().value, "keep-b");
    assert_eq!(fetcher.calls(), 1);
}

#[tokio::test]
async fn two_upsert_refreshers_share_union_identity_map_without_deleting_each_other() {
    let punnu = Punnu::<Item>::builder().build();
    let query_a = ScriptedFetcher::new([FetchOutcome::Items(vec![
        Item {
            id: 1,
            query: "overlap",
            value: "from-a",
        },
        Item {
            id: 2,
            query: "query-a",
            value: "a-only",
        },
    ])]);
    let query_b = ScriptedFetcher::new([FetchOutcome::Items(vec![
        Item {
            id: 1,
            query: "overlap",
            value: "from-b",
        },
        Item {
            id: 3,
            query: "query-b",
            value: "b-only",
        },
    ])]);
    let handle_a =
        punnu.start_periodic_refresh(Duration::from_secs(3600), query_a, RefreshMode::UpsertOnly);
    let handle_b =
        punnu.start_periodic_refresh(Duration::from_secs(3600), query_b, RefreshMode::UpsertOnly);

    handle_a.refresh_now().await.unwrap();
    handle_b.refresh_now().await.unwrap();
    handle_a.cancel();
    handle_b.cancel();

    assert_eq!(punnu.len(), 3);
    assert_eq!(punnu.get(&1).unwrap().value, "from-b");
    assert_eq!(punnu.get(&2).unwrap().value, "a-only");
    assert_eq!(punnu.get(&3).unwrap().value, "b-only");
}

#[tokio::test]
async fn refresh_now_replace_removes_absent_entries_only_for_authoritative_full_set() {
    let punnu = Punnu::<Item>::builder().build();
    punnu
        .insert(Item {
            id: 1,
            query: "full-set",
            value: "old",
        })
        .await
        .unwrap();
    punnu
        .insert(Item {
            id: 99,
            query: "other-query",
            value: "removed",
        })
        .await
        .unwrap();
    let mut events = punnu.events();

    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        ScriptedFetcher::new([FetchOutcome::Items(vec![
            Item {
                id: 1,
                query: "full-set",
                value: "new",
            },
            Item {
                id: 2,
                query: "full-set",
                value: "added",
            },
        ])]),
        RefreshMode::Replace,
    );

    handle.refresh_now().await.unwrap();
    handle.cancel();

    assert_eq!(punnu.get(&1).unwrap().value, "new");
    assert_eq!(punnu.get(&2).unwrap().value, "added");
    assert!(punnu.get(&99).is_none());

    let mut saw_removed = false;
    for _ in 0..3 {
        if let sassi::PunnuEvent::Invalidate { id, reason } = events.recv().await.unwrap() {
            saw_removed = id == 99 && reason == InvalidationReason::Manual.into();
            if saw_removed {
                break;
            }
        }
    }
    assert!(saw_removed);
}

#[tokio::test]
async fn replace_refresh_event_observers_see_final_published_state() {
    let punnu = Punnu::<Item>::builder().build();
    punnu
        .insert(Item {
            id: 1,
            query: "full-set",
            value: "old",
        })
        .await
        .unwrap();
    punnu
        .insert(Item {
            id: 99,
            query: "removed",
            value: "old",
        })
        .await
        .unwrap();
    let mut events = punnu.events();
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        ScriptedFetcher::new([FetchOutcome::Items(vec![
            Item {
                id: 1,
                query: "full-set",
                value: "new",
            },
            Item {
                id: 2,
                query: "full-set",
                value: "added",
            },
        ])]),
        RefreshMode::Replace,
    );

    let refresh = tokio::spawn(async move { handle.refresh_now().await });
    let first_event = events.recv().await.unwrap();

    assert!(matches!(
        first_event,
        sassi::PunnuEvent::Invalidate { id: 99, reason }
            if reason == InvalidationReason::Manual.into()
    ));
    assert_eq!(punnu.get(&1).unwrap().value, "new");
    assert_eq!(punnu.get(&2).unwrap().value, "added");
    assert!(punnu.get(&99).is_none());
    refresh.await.unwrap().unwrap();
}

#[tokio::test]
async fn refresh_replacement_events_follow_last_write_wins_default() {
    let punnu = Punnu::<Item>::builder().build();
    punnu
        .insert(Item {
            id: 1,
            query: "full-set",
            value: "old",
        })
        .await
        .unwrap();
    let mut events = punnu.events();
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        ScriptedFetcher::new([FetchOutcome::Items(vec![Item {
            id: 1,
            query: "full-set",
            value: "new",
        }])]),
        RefreshMode::UpsertOnly,
    );

    handle.refresh_now().await.unwrap();
    handle.cancel();

    assert!(matches!(
        events.recv().await.unwrap(),
        sassi::PunnuEvent::Insert { value } if value.value == "new"
    ));
    assert_eq!(punnu.get(&1).unwrap().value, "new");
}

#[tokio::test]
async fn refresh_replacement_events_follow_update_policy() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Update,
            ..Default::default()
        })
        .build();
    punnu
        .insert(Item {
            id: 1,
            query: "full-set",
            value: "old",
        })
        .await
        .unwrap();
    let mut events = punnu.events();
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        ScriptedFetcher::new([FetchOutcome::Items(vec![Item {
            id: 1,
            query: "full-set",
            value: "new",
        }])]),
        RefreshMode::Replace,
    );

    handle.refresh_now().await.unwrap();
    handle.cancel();

    assert!(matches!(
        events.recv().await.unwrap(),
        sassi::PunnuEvent::Update { old, new }
            if old.value == "old" && new.value == "new"
    ));
    assert_eq!(punnu.get(&1).unwrap().value, "new");
}

#[tokio::test]
async fn refresh_replacement_events_follow_reject_policy() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            on_conflict: OnConflict::Reject,
            ..Default::default()
        })
        .build();
    punnu
        .insert(Item {
            id: 1,
            query: "full-set",
            value: "old",
        })
        .await
        .unwrap();
    let mut events = punnu.events();
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        ScriptedFetcher::new([FetchOutcome::Items(vec![Item {
            id: 1,
            query: "full-set",
            value: "new",
        }])]),
        RefreshMode::UpsertOnly,
    );

    handle.refresh_now().await.unwrap();
    handle.cancel();

    assert_eq!(punnu.get(&1).unwrap().value, "old");
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test(start_paused = true)]
async fn scheduled_tick_refreshes_and_cancel_stops_future_ticks() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedFetcher::new([
        FetchOutcome::Items(vec![Item {
            id: 1,
            query: "scheduled",
            value: "first",
        }]),
        FetchOutcome::Items(vec![Item {
            id: 2,
            query: "scheduled",
            value: "second",
        }]),
    ]);
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(5),
        fetcher.clone(),
        RefreshMode::UpsertOnly,
    );

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    wait_for_calls(&fetcher, 1).await;
    assert_eq!(punnu.get(&1).unwrap().value, "first");

    handle.cancel();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(20)).await;
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }

    assert_eq!(fetcher.calls(), 1);
    assert!(punnu.get(&2).is_none());
}

#[tokio::test(start_paused = true)]
async fn cancel_is_idempotent_and_prevents_future_ticks() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedFetcher::new([FetchOutcome::Items(vec![Item {
        id: 1,
        query: "scheduled",
        value: "should-not-run",
    }])]);
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(5),
        fetcher.clone(),
        RefreshMode::UpsertOnly,
    );

    tokio::task::yield_now().await;
    handle.cancel();
    handle.cancel();
    handle.cancel();
    tokio::time::advance(Duration::from_secs(20)).await;
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }

    assert_eq!(fetcher.calls(), 0);
    assert!(punnu.is_empty());
}

#[tokio::test]
async fn refresh_now_after_cancel_returns_stopped_without_fetching() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = ScriptedFetcher::new([FetchOutcome::Items(vec![Item {
        id: 1,
        query: "manual",
        value: "should-not-run",
    }])]);
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        fetcher.clone(),
        RefreshMode::UpsertOnly,
    );

    handle.cancel();
    let err = handle.refresh_now().await.unwrap_err();

    assert!(
        matches!(err, FetchError::Serialization(message) if message.contains("refresh task stopped"))
    );
    assert_eq!(fetcher.calls(), 0);
    assert!(punnu.is_empty());
}

#[tokio::test]
async fn cancel_during_manual_refresh_drops_fetch_and_returns_stopped() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = BlockingFetcher::new();
    let handle = Arc::new(punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        fetcher.clone(),
        RefreshMode::UpsertOnly,
    ));
    let refresh = tokio::spawn({
        let handle = handle.clone();
        async move { handle.refresh_now().await }
    });

    fetcher.started.notified().await;
    handle.cancel();
    tokio::time::timeout(Duration::from_secs(1), fetcher.dropped.notified())
        .await
        .unwrap();
    let err = refresh.await.unwrap().unwrap_err();

    assert!(
        matches!(err, FetchError::Serialization(message) if message.contains("refresh task stopped"))
    );
    assert!(punnu.is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancel_during_scheduled_refresh_drops_fetch_without_mutating_l1() {
    let punnu = Punnu::<Item>::builder().build();
    let fetcher = BlockingFetcher::new();
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(5),
        fetcher.clone(),
        RefreshMode::UpsertOnly,
    );

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    fetcher.started.notified().await;
    handle.cancel();
    tokio::time::timeout(Duration::from_secs(1), fetcher.dropped.notified())
        .await
        .unwrap();

    assert!(punnu.is_empty());
}

#[tokio::test]
async fn refresh_now_error_returns_error_and_does_not_mutate_l1() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            ..Default::default()
        })
        .build();
    punnu
        .insert(Item {
            id: 1,
            query: "stable",
            value: "old",
        })
        .await
        .unwrap();
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(3600),
        ScriptedFetcher::new([FetchOutcome::Err("source unavailable")]),
        RefreshMode::Replace,
    );

    let err = handle.refresh_now().await.unwrap_err();
    handle.cancel();

    assert!(matches!(err, FetchError::Serialization(message) if message == "source unavailable"));
    assert_eq!(punnu.get(&1).unwrap().value, "old");
    assert_eq!(punnu.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn scheduled_fetch_error_does_not_stop_later_ticks_or_mutate_l1() {
    let punnu = Punnu::<Item>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            ..Default::default()
        })
        .build();
    punnu
        .insert(Item {
            id: 1,
            query: "stable",
            value: "old",
        })
        .await
        .unwrap();
    let fetcher = ScriptedFetcher::new([
        FetchOutcome::Err("source unavailable"),
        FetchOutcome::Items(vec![Item {
            id: 2,
            query: "scheduled",
            value: "recovered",
        }]),
    ]);
    let handle = punnu.start_periodic_refresh(
        Duration::from_secs(5),
        fetcher.clone(),
        RefreshMode::UpsertOnly,
    );

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    wait_for_calls(&fetcher, 1).await;
    assert_eq!(punnu.get(&1).unwrap().value, "old");
    assert!(punnu.get(&2).is_none());

    tokio::time::advance(Duration::from_secs(5)).await;
    wait_for_calls(&fetcher, 2).await;
    handle.cancel();

    assert_eq!(punnu.get(&1).unwrap().value, "old");
    assert_eq!(punnu.get(&2).unwrap().value, "recovered");
}

#[tokio::test]
#[should_panic(expected = "periodic refresh interval must be non-zero")]
async fn start_periodic_refresh_rejects_zero_interval() {
    let punnu = Punnu::<Item>::builder().build();
    let _handle = punnu.start_periodic_refresh(
        Duration::ZERO,
        ScriptedFetcher::new([FetchOutcome::Items(Vec::new())]),
        RefreshMode::UpsertOnly,
    );
}

async fn wait_for_calls(fetcher: &ScriptedFetcher, expected: usize) {
    for _ in 0..10 {
        if fetcher.calls() >= expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(fetcher.calls(), expected);
}
