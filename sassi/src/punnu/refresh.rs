//! Periodic refresh helper for simple fixed-interval polling.
//!
//! This module covers the lightweight polling path: fetch a full or
//! partial truth set on a timer and apply it to one [`Punnu`](crate::Punnu).
//! More complex live-query and delta-sync machinery lives in later
//! modules so the basic polling UX stays small.

use crate::cacheable::Cacheable;
use crate::error::FetchError;
#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, watch};

/// Handle returned by [`crate::Punnu::start_periodic_refresh`].
///
/// Dropping the handle stops future manual triggers. Call
/// [`RefreshHandle::cancel`] to also ask the background task to exit
/// before its next scheduled tick.
pub struct RefreshHandle {
    pub(crate) cancel: watch::Sender<bool>,
    pub(crate) trigger: mpsc::Sender<oneshot::Sender<Result<(), FetchError>>>,
}

impl RefreshHandle {
    /// Stop the refresh task. Idempotent; duplicate calls are ignored.
    pub fn cancel(&self) {
        let _ = self.cancel.send(true);
    }

    /// Trigger one immediate refresh and wait for its result.
    ///
    /// This runs through the same single background task as scheduled
    /// ticks, so manual refreshes cannot overlap with a tick or each
    /// other.
    ///
    /// # Errors
    ///
    /// Returns the fetch/apply error from the refresh attempt. If the
    /// background task has already exited, returns a serialization
    /// shaped error with a diagnostic message.
    pub async fn refresh_now(&self) -> Result<(), FetchError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.trigger
            .send(reply_tx)
            .await
            .map_err(|_| FetchError::Serialization("refresh task stopped".to_owned()))?;
        reply_rx.await.map_err(|_| {
            FetchError::Serialization("refresh task stopped before replying".to_owned())
        })?
    }
}

impl Drop for RefreshHandle {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

/// How fetched refresh results are applied to the resident L1 state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshMode {
    /// Insert or update fetched entries and leave absent resident ids
    /// untouched. Use for partial, paginated, filtered, or query
    /// specific pollers, as long as each id represents the same canonical
    /// payload wherever it appears.
    UpsertOnly,

    /// Treat the fetched result as the complete authoritative set for
    /// the whole `Punnu<T>`. Resident ids absent from the fetch are
    /// invalidated with [`crate::punnu::InvalidationReason::Manual`].
    ///
    /// Use only when the fetcher returns the complete truth set for this
    /// resident pool. For partial, tenant-filtered, auth-filtered, paginated, or
    /// query-specific polling, use [`RefreshMode::UpsertOnly`] or isolate the
    /// identity map with a wrapper type, tenant-qualified id, or separate
    /// `Punnu<T>`.
    Replace,
}

/// User-supplied fetcher for periodic refresh.
///
/// Implementations usually wrap an HTTP client, database query, or
/// local file read and return the current set of values that should be
/// applied to a `Punnu`.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait PunnuFetcher<T: Cacheable>: Send + Sync + 'static {
    /// Fetch values for one refresh attempt.
    async fn fetch(&self) -> Result<Vec<T>, FetchError>;
}

/// User-supplied fetcher for periodic refresh.
///
/// The wasm target accepts non-`Send` futures so browser-native
/// fetchers can await JS/gloo futures without artificial shims.
#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
pub trait PunnuFetcher<T: Cacheable>: 'static {
    /// Fetch values for one refresh attempt.
    async fn fetch(&self) -> Result<Vec<T>, FetchError>;
}
