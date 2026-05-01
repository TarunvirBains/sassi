//! Storage entry layout + TTL helpers.
//!
//! The internal LRU map stores [`Entry<T>`] rather than `Arc<T>`
//! directly so per-entry metadata (currently just `expires_at`) lives
//! alongside the value. Consumers never see `Entry<T>` — `Punnu`
//! returns `Arc<T>` from `get` / `insert`. Task 4 introduces the type;
//! Task 6 wires `expires_at` into the lazy-expiry and background
//! sweep paths.
//!
//! # Why an internal struct rather than a tuple?
//!
//! - **Forward compatibility:** later metadata (per-entry `inserted_at`
//!   for refresh hints, per-entry `tenant_origin` for cross-tenant
//!   guard diagnostics, …) lands without changing every callsite.
//! - **Discoverability:** `entry.is_expired()` reads better than
//!   `entry.1.map(|t| t <= Instant::now()).unwrap_or(false)` and keeps
//!   the comparison policy in one place.
//! - **Clone semantics:** `Entry<T>: Clone` cheaply because the
//!   payload is `Arc<T>`; `Instant` is `Copy`. Cloning happens
//!   on every `get` returning a sharable handle.

use std::sync::Arc;
use std::time::Instant;

/// LRU storage cell — holds the cached payload plus per-entry
/// metadata.
///
/// `pub(crate)` because `Punnu` returns `Arc<T>` from `get`; the
/// metadata is internal bookkeeping.
pub(crate) struct Entry<T> {
    /// Shared handle to the cached payload.
    pub value: Arc<T>,

    /// Absolute expiry deadline, computed from
    /// `Instant::now() + ttl` at insert time. `None` means the entry
    /// never expires on time (LRU eviction can still drop it).
    pub expires_at: Option<Instant>,
}

impl<T> Entry<T> {
    /// Construct an entry with no TTL. Convenience helper used by the
    /// vast majority of inserts (default config has `default_ttl =
    /// None`). The TTL-aware `with_expiry` / `is_expired_at` helpers
    /// land alongside the lazy-expiry behaviour in the next task.
    pub(crate) fn new(value: Arc<T>) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }
}

// Manual `Clone`: deriving would require `T: Clone` even though the
// only data field is `Arc<T>` (which is `Clone` regardless of `T`).
impl<T> Clone for Entry<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            expires_at: self.expires_at,
        }
    }
}
