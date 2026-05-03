//! Delta-sync refresh subscriptions.
//!
//! A delta subscription is scoped to one fetcher/filter pair. Multiple
//! subscriptions may write into the same [`Punnu`]
//! identity map, but each subscription owns its own watermark and
//! single-flight slot so a narrow query cannot advance a broader query's
//! progress.

use crate::error::FetchError;
use crate::executor::BoxFut;
use crate::punnu::delta::DeltaResult;
use crate::punnu::pool::Punnu;
use crate::punnu::single_flight::{FetchErrorClone, into_clone};
use crate::watermark::DeltaSyncCacheable;
#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;
use futures::FutureExt;
#[cfg(not(target_arch = "wasm32"))]
use futures::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures::future::LocalBoxFuture;
use futures::future::Shared;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

/// User-supplied delta fetcher for one refresh subscription.
///
/// Native fetchers must be `Send + Sync` because Sassi's default native
/// executor uses `tokio::spawn`. The wasm target accepts non-`Send`
/// futures so browser-side fetchers can await JS-backed primitives.
///
/// # Fetcher contract
///
/// Treat [`DeltaQuery::since`] as an inclusive lower bound (`>=`), not a
/// strict `>` cursor. Boundary rows may have changed without their
/// watermark changing; Sassi deduplicates by `T::Id` when those rows are
/// returned again.
///
/// Tombstones are true deletes from the shared `Punnu<T>` identity map.
/// Do not use a tombstone to mean "this row left my query/filter/page";
/// return the updated row and let read-time predicates stop matching it.
/// For delete-only batches, use
/// [`DeltaResult::with_high_watermark`] so the subscription can advance
/// its source cursor even though no item carries the delete watermark.
///
/// The subscription cursor tracks source progress, not L1 retention. A
/// row can be processed and then omitted from L1 because sampled-LRU
/// evicted it, or because [`crate::punnu::OnConflict::Reject`] kept an
/// existing resident value. Use `LastWriteWins` or `Update` when a delta
/// stream is authoritative for cached values.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait DeltaPunnuFetcher<T: DeltaSyncCacheable>: Send + Sync + 'static {
    /// Fetch one delta or full-refresh result for this subscription.
    async fn fetch_delta(
        &self,
        query: DeltaQuery<T>,
    ) -> Result<DeltaResult<T, T::Watermark>, FetchError>;
}

/// User-supplied delta fetcher for one refresh subscription.
///
/// The wasm target accepts non-`Send` futures so browser-side fetchers
/// can await JS-backed primitives.
///
/// # Fetcher contract
///
/// Treat [`DeltaQuery::since`] as an inclusive lower bound (`>=`), not a
/// strict `>` cursor. Boundary rows may have changed without their
/// watermark changing; Sassi deduplicates by `T::Id` when those rows are
/// returned again.
///
/// Tombstones are true deletes from the shared `Punnu<T>` identity map.
/// Do not use a tombstone to mean "this row left my query/filter/page";
/// return the updated row and let read-time predicates stop matching it.
/// For delete-only batches, use
/// [`DeltaResult::with_high_watermark`] so the subscription can advance
/// its source cursor even though no item carries the delete watermark.
///
/// The subscription cursor tracks source progress, not L1 retention. A
/// row can be processed and then omitted from L1 because sampled-LRU
/// evicted it, or because [`crate::punnu::OnConflict::Reject`] kept an
/// existing resident value. Use `LastWriteWins` or `Update` when a delta
/// stream is authoritative for cached values.
#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
pub trait DeltaPunnuFetcher<T: DeltaSyncCacheable>: 'static {
    /// Fetch one delta or full-refresh result for this subscription.
    async fn fetch_delta(
        &self,
        query: DeltaQuery<T>,
    ) -> Result<DeltaResult<T, T::Watermark>, FetchError>;
}

/// Query passed to [`DeltaPunnuFetcher::fetch_delta`].
#[non_exhaustive]
pub struct DeltaQuery<T: DeltaSyncCacheable> {
    /// The current subscription watermark. `None` means a full query.
    ///
    /// When this is `Some`, fetchers must query with an inclusive
    /// `>=` boundary and let Sassi deduplicate rows by identity.
    pub since: Option<T::Watermark>,
    /// IDs that must be recovered regardless of watermark.
    ///
    /// Task 14c does not populate this yet; Task 14d wires the
    /// recovery-set policies.
    pub recover_ids: HashSet<T::Id>,
}

/// Result of one delta subscription update.
pub struct UpdateResult<T: DeltaSyncCacheable> {
    /// Count of fetched items that survived into the published snapshot.
    pub applied: usize,
    /// The subscription's high-water mark after the update.
    pub watermark: Option<T::Watermark>,
}

impl<T: DeltaSyncCacheable> Clone for UpdateResult<T> {
    fn clone(&self) -> Self {
        Self {
            applied: self.applied,
            watermark: self.watermark.clone(),
        }
    }
}

impl<T> fmt::Debug for UpdateResult<T>
where
    T: DeltaSyncCacheable,
    T::Watermark: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpdateResult")
            .field("applied", &self.applied)
            .field("watermark", &self.watermark)
            .finish()
    }
}

impl<T: DeltaSyncCacheable> PartialEq for UpdateResult<T> {
    fn eq(&self, other: &Self) -> bool {
        self.applied == other.applied && self.watermark == other.watermark
    }
}

impl<T: DeltaSyncCacheable> Eq for UpdateResult<T> {}

/// Public handle to a delta-sync subscription.
pub struct DeltaRefreshHandle<T: DeltaSyncCacheable> {
    pub(crate) inner: Arc<RefreshSubscription<T>>,
}

impl<T: DeltaSyncCacheable> DeltaRefreshHandle<T> {
    /// Trigger one delta update and wait for the shared result.
    ///
    /// Once a delta fetch is registered, dropping this caller's future
    /// does not cancel the fetch or its cache-state application.
    ///
    /// # Panics
    ///
    /// On native `runtime-tokio` builds, panics if called outside an
    /// active Tokio runtime.
    pub async fn update(&self) -> Result<UpdateResult<T>, FetchError> {
        assert_active_tokio_runtime("DeltaRefreshHandle::update");
        RefreshSubscription::update(self.inner.clone()).await
    }

    /// Trigger one full refresh and wait for the shared result.
    ///
    /// If a delta is already in flight, the full refresh is queued behind
    /// it and coalesced for every caller. Once the request is registered,
    /// dropping the caller's future does not cancel the queued full
    /// refresh; this prevents `update_full()` starvation under sustained
    /// delta traffic.
    ///
    /// # Panics
    ///
    /// On native `runtime-tokio` builds, panics if called outside an
    /// active Tokio runtime.
    pub async fn update_full(&self) -> Result<UpdateResult<T>, FetchError> {
        assert_active_tokio_runtime("DeltaRefreshHandle::update_full");
        RefreshSubscription::update_full(self.inner.clone()).await
    }

    /// Stop future periodic ticks. In-flight fetches continue to
    /// completion and still apply their cache-state changes.
    pub fn cancel(&self) {
        let _ = self.inner.cancel.send(true);
    }

    /// Return the current subscription watermark.
    pub fn watermark(&self) -> Option<T::Watermark> {
        self.inner
            .last_watermark
            .lock()
            .expect("delta refresh watermark lock poisoned")
            .clone()
    }
}

impl<T: DeltaSyncCacheable> Drop for DeltaRefreshHandle<T> {
    fn drop(&mut self) {
        let _ = self.inner.cancel.send(true);
    }
}

pub(crate) struct RefreshSubscription<T: DeltaSyncCacheable> {
    punnu: Punnu<T>,
    fetcher: Arc<dyn DeltaPunnuFetcher<T>>,
    last_watermark: Mutex<Option<T::Watermark>>,
    slot: Mutex<InFlightSlot<T>>,
    next_operation_id: AtomicU64,
    cancel: watch::Sender<bool>,
}

enum InFlightSlot<T: DeltaSyncCacheable> {
    Empty,
    Delta {
        operation_id: u64,
        shared: SharedUpdate<T>,
        pending_full: Option<PendingFull<T>>,
    },
    Full {
        operation_id: u64,
        shared: SharedUpdate<T>,
    },
}

struct PendingFull<T: DeltaSyncCacheable> {
    operation_id: u64,
    shared: SharedUpdate<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TickKind {
    Delta,
    Full,
}

enum UpdateFullAction<T: DeltaSyncCacheable> {
    AwaitFull(SharedUpdate<T>),
    AwaitDeltaThenFull {
        delta: SharedUpdate<T>,
        full: SharedUpdate<T>,
    },
}

type SharedOutput<T> = Result<UpdateResult<T>, FetchErrorClone>;

#[cfg(not(target_arch = "wasm32"))]
type UpdateFuture<T> = BoxFuture<'static, SharedOutput<T>>;
#[cfg(target_arch = "wasm32")]
type UpdateFuture<T> = LocalBoxFuture<'static, SharedOutput<T>>;

type SharedUpdate<T> = Shared<UpdateFuture<T>>;

impl<T: DeltaSyncCacheable> RefreshSubscription<T> {
    pub(crate) fn spawn<F>(punnu: Punnu<T>, interval: Duration, fetcher: F) -> DeltaRefreshHandle<T>
    where
        F: DeltaPunnuFetcher<T>,
    {
        assert!(
            !interval.is_zero(),
            "delta refresh interval must be non-zero"
        );

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let subscription = Arc::new(Self {
            punnu,
            fetcher: Arc::new(fetcher),
            last_watermark: Mutex::new(None),
            slot: Mutex::new(InFlightSlot::Empty),
            next_operation_id: AtomicU64::new(1),
            cancel: cancel_tx,
        });

        let loop_subscription = subscription.clone();
        let executor = subscription.punnu.executor();
        executor.spawn(box_spawn_future(async move {
            run_periodic_delta_refresh(loop_subscription, interval, cancel_rx).await;
        }));

        DeltaRefreshHandle {
            inner: subscription,
        }
    }

    async fn update(subscription: Arc<Self>) -> Result<UpdateResult<T>, FetchError> {
        let shared = subscription.shared_for_delta();
        shared.await.map_err(FetchError::from)
    }

    async fn update_full(subscription: Arc<Self>) -> Result<UpdateResult<T>, FetchError> {
        let action = {
            let mut slot = subscription
                .slot
                .lock()
                .expect("delta refresh in-flight slot lock poisoned");
            match &mut *slot {
                InFlightSlot::Empty => {
                    let operation_id = subscription.next_operation_id();
                    let shared = subscription.build_and_spawn_tick(TickKind::Full, operation_id);
                    *slot = InFlightSlot::Full {
                        operation_id,
                        shared: shared.clone(),
                    };
                    UpdateFullAction::AwaitFull(shared)
                }
                InFlightSlot::Full { shared, .. } => {
                    let shared = shared.clone();
                    UpdateFullAction::AwaitFull(shared)
                }
                InFlightSlot::Delta {
                    shared,
                    pending_full,
                    ..
                } => {
                    let delta = shared.clone();
                    let full = if let Some(pending) = pending_full {
                        pending.shared.clone()
                    } else {
                        let operation_id = subscription.next_operation_id();
                        let full =
                            subscription.build_and_spawn_chained_full(delta.clone(), operation_id);
                        *pending_full = Some(PendingFull {
                            operation_id,
                            shared: full.clone(),
                        });
                        full
                    };
                    UpdateFullAction::AwaitDeltaThenFull { delta, full }
                }
            }
        };

        match action {
            UpdateFullAction::AwaitFull(shared) => shared.await.map_err(FetchError::from),
            UpdateFullAction::AwaitDeltaThenFull { delta, full } => {
                let _ = delta.await;
                full.await.map_err(FetchError::from)
            }
        }
    }

    fn shared_for_delta(self: &Arc<Self>) -> SharedUpdate<T> {
        let mut slot = self
            .slot
            .lock()
            .expect("delta refresh in-flight slot lock poisoned");
        match &mut *slot {
            InFlightSlot::Empty => {
                let operation_id = self.next_operation_id();
                let shared = self.build_and_spawn_tick(TickKind::Delta, operation_id);
                *slot = InFlightSlot::Delta {
                    operation_id,
                    shared: shared.clone(),
                    pending_full: None,
                };
                shared
            }
            InFlightSlot::Delta { shared, .. } | InFlightSlot::Full { shared, .. } => {
                shared.clone()
            }
        }
    }

    async fn run_tick(&self, kind: TickKind) -> Result<UpdateResult<T>, FetchError> {
        let since = match kind {
            TickKind::Delta => self
                .last_watermark
                .lock()
                .expect("delta refresh watermark lock poisoned")
                .clone(),
            TickKind::Full => None,
        };

        let delta = self
            .fetcher
            .fetch_delta(DeltaQuery {
                since,
                recover_ids: HashSet::new(),
            })
            .await?;

        let (applied, next_watermark) = self.apply_delta_and_observed_watermark(delta);
        let watermark = self.advance_watermark(next_watermark);

        Ok(UpdateResult { applied, watermark })
    }

    fn apply_delta_and_observed_watermark(
        &self,
        delta: DeltaResult<T, T::Watermark>,
    ) -> (usize, Option<T::Watermark>) {
        let next_watermark = delta.observed_watermark();
        let stats = self.punnu.apply_delta(delta.without_high_watermark());
        (stats.applied_items, next_watermark)
    }

    fn advance_watermark(&self, next_watermark: Option<T::Watermark>) -> Option<T::Watermark> {
        let mut stored = self
            .last_watermark
            .lock()
            .expect("delta refresh watermark lock poisoned");
        if let Some(next_watermark) = next_watermark {
            match &*stored {
                Some(current) if current >= &next_watermark => {}
                _ => *stored = Some(next_watermark),
            }
        }
        stored.clone()
    }

    fn finish_tick(self: Arc<Self>, kind: TickKind, operation_id: u64) {
        let mut slot = self
            .slot
            .lock()
            .expect("delta refresh in-flight slot lock poisoned");

        match (&mut *slot, kind) {
            (
                InFlightSlot::Delta {
                    operation_id: current_id,
                    pending_full,
                    ..
                },
                TickKind::Delta,
            ) if *current_id == operation_id => {
                if let Some(pending) = pending_full.take() {
                    *slot = InFlightSlot::Full {
                        operation_id: pending.operation_id,
                        shared: pending.shared,
                    };
                } else {
                    *slot = InFlightSlot::Empty;
                }
            }
            (
                InFlightSlot::Full {
                    operation_id: current_id,
                    ..
                },
                TickKind::Full,
            ) if *current_id == operation_id => {
                *slot = InFlightSlot::Empty;
            }
            _ => {}
        }
    }

    fn build_and_spawn_tick(
        self: &Arc<Self>,
        kind: TickKind,
        operation_id: u64,
    ) -> SharedUpdate<T> {
        let future = box_update_future(self.clone().run_owned_tick(kind, operation_id));
        let shared = future.shared();
        let driver = shared.clone();
        self.punnu.executor().spawn(box_spawn_future(async move {
            let _ = driver.await;
        }));
        shared
    }

    fn build_and_spawn_chained_full(
        self: &Arc<Self>,
        delta: SharedUpdate<T>,
        operation_id: u64,
    ) -> SharedUpdate<T> {
        let owner = self.clone();
        let future = box_update_future(async move {
            let _ = delta.await;
            owner.run_owned_tick(TickKind::Full, operation_id).await
        });
        let shared = future.shared();
        let driver = shared.clone();
        self.punnu.executor().spawn(box_spawn_future(async move {
            let _ = driver.await;
        }));
        shared
    }

    async fn run_owned_tick(self: Arc<Self>, kind: TickKind, operation_id: u64) -> SharedOutput<T> {
        let type_name = std::any::type_name::<T>();
        let result = AssertUnwindSafe(self.run_tick(kind)).catch_unwind().await;
        let output = match result {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => Err(into_clone(err)),
            Err(panic_payload) => Err(FetchErrorClone::FetcherPanic {
                type_name,
                message: panic_message(&panic_payload),
            }),
        };
        self.finish_tick(kind, operation_id);
        output
    }

    fn next_operation_id(&self) -> u64 {
        self.next_operation_id.fetch_add(1, Ordering::Relaxed)
    }
}

async fn run_periodic_delta_refresh<T>(
    subscription: Arc<RefreshSubscription<T>>,
    interval: Duration,
    mut cancel: watch::Receiver<bool>,
) where
    T: DeltaSyncCacheable,
{
    loop {
        if refresh_cancelled(&cancel) {
            break;
        }

        let sleep = subscription.punnu.executor().sleep(interval);
        tokio::select! {
            biased;
            changed = cancel.changed() => {
                let _ = changed;
                break;
            }
            _ = sleep => {
                if refresh_cancelled(&cancel) {
                    break;
                }
                let shared = subscription.shared_for_delta();
                tokio::select! {
                    biased;
                    changed = cancel.changed() => {
                        let _ = changed;
                        break;
                    }
                    result = shared => {
                        if let Err(err) = result.map_err(FetchError::from) {
                            tracing::warn!(error = %err, "delta refresh failed");
                        }
                    }
                }
            }
        }
    }
}

fn refresh_cancelled(cancel: &watch::Receiver<bool>) -> bool {
    *cancel.borrow() || cancel.has_changed().is_err()
}

fn assert_active_tokio_runtime(_operation: &str) {
    #[cfg(all(feature = "runtime-tokio", not(target_arch = "wasm32")))]
    if tokio::runtime::Handle::try_current().is_err() {
        panic!("{_operation} requires an active Tokio runtime");
    }
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        String::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn box_update_future<T>(
    future: impl Future<Output = SharedOutput<T>> + Send + 'static,
) -> UpdateFuture<T>
where
    T: DeltaSyncCacheable,
{
    Box::pin(future)
}

#[cfg(target_arch = "wasm32")]
fn box_update_future<T>(future: impl Future<Output = SharedOutput<T>> + 'static) -> UpdateFuture<T>
where
    T: DeltaSyncCacheable,
{
    Box::pin(future)
}

#[cfg(not(target_arch = "wasm32"))]
fn box_spawn_future(future: impl Future<Output = ()> + Send + 'static) -> BoxFut<'static> {
    Box::pin(future)
}

#[cfg(target_arch = "wasm32")]
fn box_spawn_future(future: impl Future<Output = ()> + 'static) -> BoxFut<'static> {
    Box::pin(future)
}

#[cfg(all(test, feature = "runtime-tokio", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::Cacheable;
    use std::collections::HashSet;
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct TestItem {
        id: i64,
        updated_at: i64,
    }

    impl Cacheable for TestItem {
        type Id = i64;
        type Fields = ();

        fn id(&self) -> Self::Id {
            self.id
        }

        fn fields() -> Self::Fields {}
    }

    impl DeltaSyncCacheable for TestItem {
        type Watermark = i64;

        fn watermark(&self) -> Self::Watermark {
            self.updated_at
        }
    }

    #[derive(Clone)]
    struct BlockingFetcher {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl DeltaPunnuFetcher<TestItem> for BlockingFetcher {
        async fn fetch_delta(
            &self,
            _query: DeltaQuery<TestItem>,
        ) -> Result<DeltaResult<TestItem, i64>, FetchError> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(DeltaResult::new(
                vec![TestItem {
                    id: 1,
                    updated_at: 10,
                }],
                HashSet::new(),
            ))
        }
    }

    async fn wait_for_notification(notify: &Notify, context: &'static str) {
        tokio::time::timeout(Duration::from_secs(2), notify.notified())
            .await
            .expect(context);
    }

    #[tokio::test]
    async fn periodic_cancel_drops_awaiter_while_spawned_fetch_continues() {
        let punnu = Punnu::<TestItem>::builder().build();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handle = punnu.start_delta_refresh(
            Duration::from_millis(1),
            BlockingFetcher {
                started: started.clone(),
                release: release.clone(),
            },
        );
        let weak_subscription = Arc::downgrade(&handle.inner);

        wait_for_notification(&started, "periodic delta fetch should start").await;
        drop(handle);

        let cancel_observed = tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                if weak_subscription.strong_count() <= 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        release.notify_one();

        assert!(
            cancel_observed.is_ok(),
            "periodic loop should stop awaiting a blocked shared update when the handle is dropped"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if weak_subscription.strong_count() == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached fetch driver should finish and release the subscription");
    }
}
