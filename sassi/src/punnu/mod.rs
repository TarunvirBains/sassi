//! [`Punnu<T>`] — typed in-memory pool with composable predicate
//! filtering, an event stream, and (later tasks) single-flight fetch
//! coalescing, opt-in TTL, and pluggable L2 backends.
//!
//! See the spec at `docs/superpowers/specs/2026-04-30-sassi-design.md`
//! §3.5 for the public-API surface, §3.5.1 for the single-flight +
//! per-process invariants, and §6 for the invalidation contract.
//!
//! # Cluster A scope
//!
//! This cluster (Tasks 4-6 of the v0.1.0 plan) lands the core pool
//! shape: identity-map storage with LRU eviction, the broadcast event
//! stream, and TTL-driven expiry (lazy + opt-in background sweep).
//! Single-flight, scopes, backends, executors, refresh, and the
//! orchestrator surface ship in subsequent clusters.

pub mod config;
pub mod events;
pub mod pool;
pub mod tenant;
pub(crate) mod ttl;

pub use config::{BackendFailureMode, CacheTier, OnConflict, PunnuConfig, PunnuMetrics};
pub use events::{EventReason, InvalidationReason, PunnuEvent};
pub use pool::{Punnu, PunnuBuilder};
pub use tenant::TenantKey;
