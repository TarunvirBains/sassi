//! [`Punnu<T>`] — typed in-memory pool with composable predicate
//! filtering, an event stream, single-flight fetch coalescing,
//! opt-in TTL, and pluggable L2 backends.
//!
//! The public adopter docs in the repository `docs/` directory describe the
//! cache model, refresh boundaries, backends, and current release surface.

pub mod config;
pub mod delta;
pub mod delta_refresh;
pub mod events;
pub(crate) mod eviction;
pub mod pool;
pub(crate) mod recovery;
pub mod refresh;
pub mod scope;
pub(crate) mod single_flight;
pub(crate) mod state;
pub mod tenant;
pub(crate) mod ttl;
pub(crate) mod write;

pub use config::{BackendFailureMode, CacheTier, OnConflict, PunnuConfig, PunnuMetrics};
pub use delta::{DeltaApplyStats, DeltaResult};
pub use delta_refresh::{DeltaPunnuFetcher, DeltaQuery, DeltaRefreshHandle, UpdateResult};
pub use events::{EventReason, InvalidationReason, PunnuEvent};
pub use pool::{Punnu, PunnuBuilder};
pub use refresh::{PunnuFetcher, RefreshHandle, RefreshMode};
pub use scope::PunnuScope;
pub use tenant::TenantKey;
