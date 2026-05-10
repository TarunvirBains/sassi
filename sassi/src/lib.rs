//! # Sassi
//!
//! Typed in-memory pool (`Punnu<T>`) with composable predicate algebra
//! (`BasicPredicate<T>` + `MemQ<T>`) and cross-runtime trait queries.
//!
//! Sassi is framework-neutral: usable from native services, workers,
//! libraries, and `wasm32-unknown-unknown` applications without a
//! backend-specific dependency. Predicates compose with `&`, `|`, `^`,
//! `!` operators and evaluate through the same in-memory path on every
//! supported target.
//!
//! Pre-v0.1.0 beta. The core public surface is available now:
//! [`Cacheable`] identities, [`Punnu<T>`](Punnu) pools, in-memory
//! [`MemQ`] scopes, lazy fetch helpers, TTL/LRU policy, event streams,
//! and atomic delta application.
//!
//! # Quick tour
//!
//! ```
//! use sassi::{Cacheable, Field, MemQ, Punnu};
//!
//! #[derive(Clone)]
//! struct User { id: i64, age: u32 }
//!
//! #[derive(Default)]
//! struct UserFields {
//!     pub id: Field<User, i64>,
//!     pub age: Field<User, u32>,
//! }
//!
//! impl Cacheable for User {
//!     type Id = i64;
//!     type Fields = UserFields;
//!
//!     fn id(&self) -> i64 { self.id }
//!
//!     fn fields() -> UserFields {
//!         UserFields {
//!             id: Field::new("id", |u| &u.id),
//!             age: Field::new("age", |u| &u.age),
//!         }
//!     }
//! }
//!
//! # let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! # rt.block_on(async {
//! let users = Punnu::<User>::builder().build();
//! users.insert(User { id: 1, age: 32 }).await.unwrap();
//!
//! let adults = users
//!     .scope(vec![MemQ::filter_basic(User::fields().age.gte(18))])
//!     .collect();
//! assert_eq!(adults.len(), 1);
//! # });
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(feature = "serde")]
pub mod backend;
pub mod cacheable;
pub mod error;
pub(crate) mod executor;
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
pub use predicate::{
    BasicPredicate, FieldPredicate, IntoBasicPredicate, LookupOp, MemQ, PresentField,
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
}

/// The crate version, surfaced from `CARGO_PKG_VERSION`. Useful for
/// runtime diagnostics. Sassi's binary wire container uses its own
/// `wire::WIRE_FORMAT_MAJOR`, not the crate semver version.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
