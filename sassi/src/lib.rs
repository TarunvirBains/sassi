//! # Sassi
//!
//! Sassi is a typed cache substrate for Rust applications with composable
//! predicate algebra and cross-runtime trait queries. It is built for when
//! cached Rust data stops being just a key-value lookup and starts becoming
//! typed local application state.
//!
//! It provides an in-memory pool ([`Punnu<T>`](Punnu)) that lets you
//! look up domain objects by identity, refresh them, and query a local
//! view using composable predicate filtering ([`BasicPredicate<T>`] + [`MemQ`])
//! —without tying that view to an ORM, a web framework, or a database client.
//!
//! Sassi is framework-neutral: usable from native services, workers,
//! libraries, and `wasm32-unknown-unknown` applications without a
//! backend-specific dependency. Predicates compose with `&`, `|`, `^`,
//! `!` operators and evaluate through the same in-memory path on every
//! supported target.
//!
//! Beta release (v0.1.0-beta.4). The core public surface is available now:
//! [`Cacheable`] identities, pools, in-memory scopes, lazy fetch helpers,
//! TTL/LRU policy, event streams, and atomic delta application.
//!
//! # Quick tour
//!
//! The same body is mirrored in [`sassi/examples/quick_tour.rs`][example]
//! (CI-verified by the workspace `cargo clippy --all-targets` gate) and in
//! the lead `README.md`.
//!
//! [example]: https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/sassi/examples/quick_tour.rs
//!
//! ```
//! use sassi::{Cacheable, MemQ, Punnu};
//!
//! #[derive(sassi::Cacheable)]
//! struct User {
//!     id: i64,
//!     age: u32,
//!     is_active: bool,
//! }
//!
//! async fn run() {
//!     // 1. Build the in-memory pool and populate it.
//!     let users = Punnu::<User>::builder().build();
//!     users
//!         .insert(User { id: 1, age: 32, is_active: true })
//!         .await
//!         .unwrap();
//!
//!     // 2. Query local state with composable predicates.
//!     let adults = users
//!         .scope(vec![MemQ::filter_basic(
//!             User::fields().age.gte(18) & User::fields().is_active.eq(true),
//!         )])
//!         .take(10)
//!         .collect();
//!     assert_eq!(adults.len(), 1);
//! }
//! # let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! # rt.block_on(run());
//! ```
//!
//! The derive requires a field literally named `id`; types whose identifier
//! uses a different name (e.g. `user_id`) must hand-implement [`Cacheable`]
//! until v0.2 adds `#[cacheable(id)]`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(feature = "serde")]
pub mod backend;
pub mod cacheable;
pub mod error;
pub(crate) mod executor;
pub mod jsahibon;
pub mod predicate;
pub mod punnu;
pub mod sassi;
mod time;
pub mod watermark;
#[cfg(feature = "serde")]
pub mod wire;

#[cfg(feature = "serde")]
pub use backend::{
    BackendInvalidation, BackendInvalidationStream, BackendKeyspace, CacheBackend, FileBackend,
    MemoryBackend,
};
pub use cacheable::{Cacheable, Field};
pub use error::{BackendError, FetchError, InsertError};
#[cfg(feature = "serde")]
pub use error::{PunnuSnapshotError, WireFormatError};
pub use jsahibon::{JFiniteF64, JObject, JSahibON, JSahibONError};
pub use predicate::{
    BasicPredicate, FieldPredicate, IntoBasicPredicate, JCompareOp, JInPolarity, JOrderedScalar,
    JPath, JSahibONFieldRef, JSahibONOptionFieldRef, JSahibONPathRef, JSahibONPredicateBody,
    JSahibONValueRef, JScalar, JScalarKind, JScalarValue, JTypeKind, LookupOp, MemQ, PresentField,
    evaluate_jsahibon_predicate,
};
pub use punnu::{
    BackendFailureMode, CacheTier, DeltaApplyStats, DeltaPunnuFetcher, DeltaQuery,
    DeltaRefreshHandle, DeltaResult, EventReason, InvalidationReason, OnConflict, Punnu,
    PunnuBuilder, PunnuConfig, PunnuEvent, PunnuFetcher, PunnuMetrics, PunnuScope, RefreshHandle,
    RefreshMode, TenantKey, UpdateResult,
};
#[cfg(feature = "serde")]
pub use punnu::{PunnuRestoreStats, SnapshotMode};
pub use sassi::Sassi;
pub use time::Instant;
pub use watermark::{DeltaSyncCacheable, MonotonicWatermark};

// Derive macro re-export. The trait and the derive share the name
// `Cacheable` (different namespaces — type namespace for the trait,
// macro namespace for the derive); this matches the standard pattern
// used by stdlib `Clone`, `Debug`, etc.
pub use sassi_macros::{Cacheable, trait_impl};

/// Implementation details used by sassi's proc macros.
///
/// This module is not part of the stable public API. Macro expansion
/// paths are routed through it so generated code does not depend on
/// private module layout.
#[doc(hidden)]
pub mod __private {
    pub use crate::sassi::trait_registry::TraitImplEntry;
    pub use inventory;
    #[cfg(feature = "serde")]
    pub use serde;
    #[cfg(feature = "serde-json-bridge")]
    pub use serde_json;
}

/// The crate version, surfaced from `CARGO_PKG_VERSION`. Useful for
/// runtime diagnostics. Sassi's binary wire container uses its own
/// `wire::WIRE_FORMAT_MAJOR`, not the crate semver version.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
