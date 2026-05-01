//! Predicate algebras.
//!
//! Sassi provides two layered predicate types:
//!
//! - [`BasicPredicate<T>`] — the universal base. Composes via `&`, `|`,
//!   `^`, `!` operators. Lowers cleanly to SQL when consumed by an ORM
//!   that knows the `Field<T, V>` shape (e.g., djogi). Evaluates
//!   identically against an in-memory `&T` via [`BasicPredicate::evaluate`].
//! - [`MemQ<T>`] — the in-memory-only extension. Adds Rust closures
//!   and sequence operations that can't be projected into SQL.
//!
//! This module owns both predicate layers. `MemQ` pairs with
//! [`crate::punnu::PunnuScope`] for in-memory execution.

pub mod basic;
pub mod field_ext;
pub mod field_predicate;
pub mod memq;

pub use basic::BasicPredicate;
pub use field_predicate::{FieldPredicate, LookupOp};
pub use memq::MemQ;
