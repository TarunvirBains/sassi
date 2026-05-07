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
use crate::punnu::events::{EventReason, PunnuEvent};
use crate::punnu::pool::Punnu;
use crate::punnu::recovery::{RecoverySet, RecoverySnapshot};
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
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;
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
    /// Eviction recovery uses this to ask the fetcher for IDs that
    /// this subscription previously observed but L1 later evicted.
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

    /// Configure eviction-triggered recovery for this subscription.
    ///
    /// When enabled, LRU evictions for IDs this subscription has
    /// observed are passed to the fetcher as
    /// [`DeltaQuery::recover_ids`] on a later delta update.
    pub fn with_eviction_recovery(self, enabled: bool) -> Self {
        self.inner
            .eviction_recovery_enabled
            .store(enabled, Ordering::Release);
        if !enabled {
            self.inner
                .recovery
                .lock()
                .expect("delta refresh recovery lock poisoned")
                .clear();
            self.inner.satisfied_force_full_generation.store(
                self.inner.force_full_generation.load(Ordering::Acquire),
                Ordering::Release,
            );
        }
        self
    }

    /// Configure periodic full refreshes.
    ///
    /// `Some(n)` makes every nth successful scheduled refresh tick use
    /// `since = None`; `None` disables the policy.
    pub fn with_periodic_full_refresh(self, every_n_ticks: Option<NonZeroUsize>) -> Self {
        self.inner.periodic_full_every.store(
            every_n_ticks.map(NonZeroUsize::get).unwrap_or(0),
            Ordering::Release,
        );
        self.inner
            .periodic_full_progress
            .store(0, Ordering::Release);
        self
    }

    /// Count IDs queued for eviction recovery on this subscription.
    ///
    /// Returns zero when eviction recovery is disabled.
    pub fn pending_eviction_recovery_count(&self) -> usize {
        if !self.inner.eviction_recovery_enabled.load(Ordering::Acquire) {
            return 0;
        }
        self.inner
            .recovery
            .lock()
            .expect("delta refresh recovery lock poisoned")
            .len()
    }

    /// Current periodic full-refresh progress.
    ///
    /// Returns `None` when the policy is disabled; otherwise returns
    /// `(elapsed, configured_every)`.
    pub fn periodic_full_refresh_progress(&self) -> Option<(usize, NonZeroUsize)> {
        let every = NonZeroUsize::new(self.inner.periodic_full_every.load(Ordering::Acquire))?;
        let progress = self
            .inner
            .periodic_full_progress
            .load(Ordering::Acquire)
            .min(every.get().saturating_sub(1));
        Some((progress, every))
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
    membership: Mutex<HashSet<T::Id>>,
    recovery: Mutex<RecoverySet<T::Id>>,
    eviction_recovery_enabled: AtomicBool,
    force_full_generation: AtomicU64,
    satisfied_force_full_generation: AtomicU64,
    lru_warning_issued: AtomicBool,
    periodic_full_every: AtomicUsize,
    periodic_full_progress: AtomicUsize,
    recovery_tick: AtomicU64,
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

struct MembershipUpdate<Id> {
    full_refresh: bool,
    item_ids: HashSet<Id>,
    tombstones: HashSet<Id>,
    recovered_ids: HashSet<Id>,
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

impl<Id> MembershipUpdate<Id>
where
    Id: Eq + std::hash::Hash + Clone,
{
    fn from_delta<T>(
        kind: TickKind,
        delta: &DeltaResult<T, T::Watermark>,
        recovery_snapshot: &RecoverySnapshot<T::Id>,
    ) -> MembershipUpdate<T::Id>
    where
        T: DeltaSyncCacheable<Id = Id>,
    {
        let tombstones = delta.tombstones.clone();
        let item_ids = delta
            .items
            .iter()
            .map(|item| item.id())
            .filter(|id| !tombstones.contains(id))
            .collect();
        MembershipUpdate {
            full_refresh: matches!(kind, TickKind::Full),
            item_ids,
            tombstones,
            recovered_ids: recovery_snapshot.ids(),
        }
    }
}

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
            recovery: Mutex::new(RecoverySet::new(punnu.config().lru_size)),
            punnu,
            fetcher: Arc::new(fetcher),
            last_watermark: Mutex::new(None),
            membership: Mutex::new(HashSet::new()),
            eviction_recovery_enabled: AtomicBool::new(false),
            force_full_generation: AtomicU64::new(0),
            satisfied_force_full_generation: AtomicU64::new(0),
            lru_warning_issued: AtomicBool::new(false),
            periodic_full_every: AtomicUsize::new(0),
            periodic_full_progress: AtomicUsize::new(0),
            recovery_tick: AtomicU64::new(1),
            slot: Mutex::new(InFlightSlot::Empty),
            next_operation_id: AtomicU64::new(1),
            cancel: cancel_tx,
        });

        let loop_subscription = subscription.clone();
        let executor = subscription.punnu.executor();
        let events_subscription = subscription.clone();
        let events = subscription.punnu.events();
        let event_cancel_rx = cancel_rx.clone();
        executor.spawn(box_spawn_future(async move {
            run_delta_recovery_event_listener(events_subscription, events, event_cancel_rx).await;
        }));
        executor.spawn(box_spawn_future(async move {
            run_periodic_delta_refresh(loop_subscription, interval, cancel_rx).await;
        }));

        DeltaRefreshHandle {
            inner: subscription,
        }
    }

    async fn update(subscription: Arc<Self>) -> Result<UpdateResult<T>, FetchError> {
        let (_, result) = Self::update_with_kind(subscription).await?;
        Ok(result)
    }

    async fn update_with_kind(
        subscription: Arc<Self>,
    ) -> Result<(TickKind, UpdateResult<T>), FetchError> {
        if subscription.delta_should_promote_to_full() {
            return Self::update_full(subscription)
                .await
                .map(|result| (TickKind::Full, result));
        }
        let (kind, shared) = subscription.shared_for_delta();
        shared
            .await
            .map(|result| (kind, result))
            .map_err(FetchError::from)
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

    fn shared_for_delta(self: &Arc<Self>) -> (TickKind, SharedUpdate<T>) {
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
                (TickKind::Delta, shared)
            }
            InFlightSlot::Delta { shared, .. } => (TickKind::Delta, shared.clone()),
            InFlightSlot::Full { shared, .. } => (TickKind::Full, shared.clone()),
        }
    }

    async fn run_tick(&self, kind: TickKind) -> Result<UpdateResult<T>, FetchError> {
        let current_tick = self.recovery_tick.fetch_add(1, Ordering::Relaxed);
        let observed_force_generation = if matches!(kind, TickKind::Full) {
            self.force_full_generation.load(Ordering::Acquire)
        } else {
            0
        };
        let (recovery_snapshot, recover_ids) = self.prepare_recovery_query(kind, current_tick);
        let mut rollback = TickRollbackGuard::new(self, current_tick, recovery_snapshot);
        let since = match kind {
            TickKind::Delta => self
                .last_watermark
                .lock()
                .expect("delta refresh watermark lock poisoned")
                .clone(),
            TickKind::Full => None,
        };

        let fetch = AssertUnwindSafe(self.fetcher.fetch_delta(DeltaQuery { since, recover_ids }))
            .catch_unwind()
            .await;

        let delta = match fetch {
            Ok(Ok(delta)) => delta,
            Ok(Err(err)) => {
                rollback.restore_after_failed();
                return Err(err);
            }
            Err(panic_payload) => {
                rollback.restore_after_failed();
                return Err(FetchError::FetcherPanic {
                    type_name: std::any::type_name::<T>(),
                    message: panic_message(&panic_payload),
                });
            }
        };

        let membership_update =
            MembershipUpdate::<T::Id>::from_delta(kind, &delta, rollback.recovery_snapshot());
        let membership_before_prime = self.membership_snapshot();
        self.prime_membership_for_observed_items(&membership_update);
        rollback.record_membership_snapshot(membership_before_prime);
        let (applied, next_watermark) =
            match self.apply_delta_and_observed_watermark(delta, rollback.recovery_snapshot()) {
                Ok(applied) => applied,
                Err(err) => {
                    rollback.restore_after_failed();
                    return Err(err);
                }
            };
        self.queue_missing_observed_items(&membership_update);
        let watermark = self.advance_watermark(next_watermark);
        self.note_successful_tick(kind, observed_force_generation);
        self.apply_membership_update(membership_update);
        rollback.note_success();

        Ok(UpdateResult { applied, watermark })
    }

    fn prepare_recovery_query(
        &self,
        kind: TickKind,
        current_tick: u64,
    ) -> (RecoverySnapshot<T::Id>, HashSet<T::Id>) {
        if !self.eviction_recovery_enabled.load(Ordering::Acquire) {
            if matches!(kind, TickKind::Full) {
                self.satisfied_force_full_generation.store(
                    self.force_full_generation.load(Ordering::Acquire),
                    Ordering::Release,
                );
            }
            return (RecoverySnapshot::empty(), HashSet::new());
        }

        let mut recovery = self
            .recovery
            .lock()
            .expect("delta refresh recovery lock poisoned");
        match kind {
            TickKind::Delta => {
                let snapshot = recovery.snapshot_eligible(current_tick);
                let recover_ids = snapshot.ids();
                (snapshot, recover_ids)
            }
            TickKind::Full => (recovery.snapshot_all(), HashSet::new()),
        }
    }

    fn restore_recovery_after_failed(&self, snapshot: RecoverySnapshot<T::Id>, current_tick: u64) {
        self.recovery
            .lock()
            .expect("delta refresh recovery lock poisoned")
            .restore_after_failed(snapshot, current_tick);
    }

    fn delta_should_promote_to_full(&self) -> bool {
        if !self.eviction_recovery_enabled.load(Ordering::Acquire) {
            return false;
        }
        if self.force_full_generation.load(Ordering::Acquire)
            > self.satisfied_force_full_generation.load(Ordering::Acquire)
        {
            return true;
        }
        self.recovery
            .lock()
            .expect("delta refresh recovery lock poisoned")
            .is_overflowing()
    }

    fn next_periodic_tick_is_full(&self) -> bool {
        let every = self.periodic_full_every.load(Ordering::Acquire);
        every != 0
            && self
                .periodic_full_progress
                .load(Ordering::Acquire)
                .saturating_add(1)
                >= every
    }

    fn note_successful_tick(&self, kind: TickKind, observed_force_generation: u64) {
        if matches!(kind, TickKind::Full) {
            self.satisfied_force_full_generation
                .fetch_max(observed_force_generation, Ordering::AcqRel);
            if self.periodic_full_every.load(Ordering::Acquire) != 0 {
                self.periodic_full_progress.store(0, Ordering::Release);
            }
        }
    }

    fn note_successful_scheduled_delta_tick(&self) {
        let every = self.periodic_full_every.load(Ordering::Acquire);
        if every == 0 {
            return;
        }
        let next = self
            .periodic_full_progress
            .load(Ordering::Acquire)
            .saturating_add(1)
            .min(every.saturating_sub(1));
        self.periodic_full_progress.store(next, Ordering::Release);
    }

    fn prime_membership_for_observed_items(&self, update: &MembershipUpdate<T::Id>) {
        if update.item_ids.is_empty() && update.tombstones.is_empty() {
            return;
        }
        let mut membership = self
            .membership
            .lock()
            .expect("delta refresh membership lock poisoned");
        for id in &update.tombstones {
            membership.remove(id);
        }
        membership.extend(update.item_ids.iter().cloned());
    }

    fn membership_snapshot(&self) -> HashSet<T::Id> {
        self.membership
            .lock()
            .expect("delta refresh membership lock poisoned")
            .clone()
    }

    fn restore_membership(&self, snapshot: HashSet<T::Id>) {
        *self
            .membership
            .lock()
            .expect("delta refresh membership lock poisoned") = snapshot;
    }

    fn queue_missing_observed_items(&self, update: &MembershipUpdate<T::Id>) {
        let missing = update
            .item_ids
            .iter()
            .filter(|id| !self.punnu.contains_unexpired(id))
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return;
        }
        let mut recovery = self
            .recovery
            .lock()
            .expect("delta refresh recovery lock poisoned");
        for id in missing {
            recovery.record_eviction(id);
        }
    }

    fn apply_membership_update(&self, update: MembershipUpdate<T::Id>) {
        let mut membership = self
            .membership
            .lock()
            .expect("delta refresh membership lock poisoned");
        if update.full_refresh {
            *membership = update.item_ids;
        } else {
            for id in update.recovered_ids {
                if !update.item_ids.contains(&id) && !update.tombstones.contains(&id) {
                    membership.remove(&id);
                }
            }
            for id in update.tombstones {
                membership.remove(&id);
            }
            membership.extend(update.item_ids);
        }
    }

    fn note_lru_eviction(&self, id: T::Id) {
        if !self.lru_warning_issued.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                "Punnu LRU eviction observed while a delta refresh subscription is active; \
                 consider raising lru_size, enabling eviction recovery, or configuring periodic full refresh"
            );
        }

        let owned_by_subscription = self
            .membership
            .lock()
            .expect("delta refresh membership lock poisoned")
            .contains(&id);
        if owned_by_subscription {
            self.recovery
                .lock()
                .expect("delta refresh recovery lock poisoned")
                .record_eviction(id);
        }
    }

    fn note_event_lag(&self, skipped: u64) {
        tracing::warn!(
            skipped,
            "delta refresh subscription event stream lagged; forcing next recovery tick to full refresh"
        );
        self.force_full_generation.fetch_add(1, Ordering::AcqRel);
    }

    fn apply_delta_and_observed_watermark(
        &self,
        delta: DeltaResult<T, T::Watermark>,
        recovery_snapshot: &RecoverySnapshot<T::Id>,
    ) -> Result<(usize, Option<T::Watermark>), FetchError> {
        let next_watermark = delta.observed_watermark();
        let recovered_ids = recovery_snapshot.ids();
        let stats = self
            .punnu
            .apply_delta_recovering(delta.without_high_watermark(), &recovered_ids);
        if stats.backend_reserved_skips > 0 {
            return Err(FetchError::Serialization(
                "delta refresh deferred because a strict backend insert reserved one of its ids"
                    .to_owned(),
            ));
        }
        Ok((stats.applied_items, next_watermark))
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

struct TickRollbackGuard<'a, T: DeltaSyncCacheable> {
    subscription: &'a RefreshSubscription<T>,
    current_tick: u64,
    recovery_snapshot: Option<RecoverySnapshot<T::Id>>,
    membership_before_prime: Option<HashSet<T::Id>>,
    resolved: bool,
}

impl<'a, T: DeltaSyncCacheable> TickRollbackGuard<'a, T> {
    fn new(
        subscription: &'a RefreshSubscription<T>,
        current_tick: u64,
        recovery_snapshot: RecoverySnapshot<T::Id>,
    ) -> Self {
        Self {
            subscription,
            current_tick,
            recovery_snapshot: Some(recovery_snapshot),
            membership_before_prime: None,
            resolved: false,
        }
    }

    fn recovery_snapshot(&self) -> &RecoverySnapshot<T::Id> {
        self.recovery_snapshot
            .as_ref()
            .expect("delta tick recovery snapshot already resolved")
    }

    fn record_membership_snapshot(&mut self, snapshot: HashSet<T::Id>) {
        self.membership_before_prime = Some(snapshot);
    }

    fn restore_after_failed(&mut self) {
        if let Some(snapshot) = self.membership_before_prime.take() {
            self.subscription.restore_membership(snapshot);
        }
        if let Some(snapshot) = self.recovery_snapshot.take() {
            self.subscription
                .restore_recovery_after_failed(snapshot, self.current_tick);
        }
        self.resolved = true;
    }

    fn note_success(mut self) {
        let Some(snapshot) = self.recovery_snapshot.take() else {
            self.resolved = true;
            return;
        };
        self.subscription
            .recovery
            .lock()
            .expect("delta refresh recovery lock poisoned")
            .note_success(snapshot);
        self.resolved = true;
    }
}

impl<T: DeltaSyncCacheable> Drop for TickRollbackGuard<'_, T> {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }

        if let Some(snapshot) = self.membership_before_prime.take() {
            match self.subscription.membership.lock() {
                Ok(mut membership) => *membership = snapshot,
                Err(_) => tracing::error!(
                    "delta refresh membership lock poisoned while rolling back failed tick"
                ),
            }
        }

        if let Some(snapshot) = self.recovery_snapshot.take() {
            match self.subscription.recovery.lock() {
                Ok(mut recovery) => {
                    recovery.restore_after_failed(snapshot, self.current_tick);
                }
                Err(_) => tracing::error!(
                    "delta refresh recovery lock poisoned while rolling back failed tick"
                ),
            }
        }
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
                tokio::select! {
                    biased;
                    changed = cancel.changed() => {
                        let _ = changed;
                        break;
                    }
                    result = run_periodic_tick(subscription.clone()) => {
                        if let Err(err) = result {
                            tracing::warn!(error = %err, "delta refresh failed");
                        }
                    }
                }
            }
        }
    }
}

async fn run_periodic_tick<T>(
    subscription: Arc<RefreshSubscription<T>>,
) -> Result<UpdateResult<T>, FetchError>
where
    T: DeltaSyncCacheable,
{
    if subscription.next_periodic_tick_is_full() {
        RefreshSubscription::update_full(subscription).await
    } else {
        let (kind, result) = RefreshSubscription::update_with_kind(subscription.clone()).await?;
        if matches!(kind, TickKind::Delta) {
            subscription.note_successful_scheduled_delta_tick();
        }
        Ok(result)
    }
}

async fn run_delta_recovery_event_listener<T>(
    subscription: Arc<RefreshSubscription<T>>,
    mut events: broadcast::Receiver<PunnuEvent<T>>,
    mut cancel: watch::Receiver<bool>,
) where
    T: DeltaSyncCacheable,
{
    loop {
        if refresh_cancelled(&cancel) {
            break;
        }

        tokio::select! {
            biased;
            changed = cancel.changed() => {
                let _ = changed;
                break;
            }
            event = events.recv() => {
                match event {
                    Ok(PunnuEvent::Invalidate { id, reason: EventReason::LruEvict }) => {
                        subscription.note_lru_eviction(id);
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        subscription.note_event_lag(skipped);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
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
