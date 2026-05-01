//! # Sassi
//!
//! Typed in-memory pool (`Punnu<T>`) with composable predicate algebra
//! (`BasicPredicate<T>` + `MemQ<T>`) and cross-runtime trait queries.
//!
//! Sassi is **runtime-agnostic** — usable from a backend (e.g., Axum +
//! a database ORM consumer like djogi) and from a Dioxus frontend
//! without any backend dependency. Predicates compose with `&`, `|`,
//! `^`, `!` operators and run identically on both runtimes.
//!
//! Pre-v0.1.0 alpha. Public surface lands incrementally per the
//! implementation plan; this version exposes the [`Cacheable`] trait
//! and [`Field`] accessor that the rest of the surface builds on.
//!
//! # Quick tour (preview)
//!
//! ```no_run
//! # // no_run because Punnu doesn't exist yet — preview only
//! use sassi::Field;
//! struct User { id: i64, age: u32 }
//! #[derive(Default)]
//! struct UserFields {
//!     pub id: Field<User, i64>,
//!     pub age: Field<User, u32>,
//! }
//! impl sassi::Cacheable for User {
//!     type Id = i64;
//!     type Fields = UserFields;
//!     fn id(&self) -> i64 { self.id }
//!     fn fields() -> UserFields {
//!         UserFields {
//!             id: Field::new("id", |u| &u.id),
//!             age: Field::new("age", |u| &u.age),
//!         }
//!     }
//! }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod cacheable;
pub mod error;
pub(crate) mod executor;
pub mod predicate;
pub mod punnu;
pub(crate) mod time;

pub use cacheable::{Cacheable, Field};
pub use error::{BackendError, InsertError};
pub use predicate::{BasicPredicate, FieldPredicate, LookupOp};
pub use punnu::{
    BackendFailureMode, CacheTier, EventReason, InvalidationReason, OnConflict, Punnu,
    PunnuBuilder, PunnuConfig, PunnuEvent, PunnuMetrics, TenantKey,
};

// Derive macro re-export. The trait and the derive share the name
// `Cacheable` (different namespaces — type namespace for the trait,
// macro namespace for the derive); this matches the standard pattern
// used by stdlib `Clone`, `Debug`, etc.
pub use sassi_macros::Cacheable;

/// The crate version, surfaced from `CARGO_PKG_VERSION`. Useful for
/// runtime diagnostics and for producing `__sassi_v` envelope tags
/// (see the wire-format module, landing in a later task).
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
