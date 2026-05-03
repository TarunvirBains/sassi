//! [`Punnu<T>`] — typed in-memory pool with composable predicate
//! filtering, an event stream, single-flight fetch coalescing,
//! opt-in TTL, and pluggable L2 backends.
//!
//! See the spec at `docs/superpowers/specs/2026-04-30-sassi-design.md`
//! §3.5 for the public-API surface, §3.5.1 for the single-flight +
//! per-process invariants, and §6 for the invalidation contract.

pub mod config;
pub mod delta;
pub mod events;
pub(crate) mod eviction;
pub mod pool;
pub mod refresh;
pub mod scope;
pub(crate) mod single_flight;
pub(crate) mod state;
pub mod tenant;
pub(crate) mod ttl;
pub(crate) mod write;

pub use config::{BackendFailureMode, CacheTier, OnConflict, PunnuConfig, PunnuMetrics};
pub use delta::{DeltaApplyStats, DeltaResult};
pub use events::{EventReason, InvalidationReason, PunnuEvent};
pub use pool::{Punnu, PunnuBuilder};
pub use refresh::{PunnuFetcher, RefreshHandle, RefreshMode};
pub use scope::PunnuScope;
pub use tenant::TenantKey;
