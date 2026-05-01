//! Predicate algebras.
//!
//! Sassi provides two layered predicate types:
//!
//! - [`BasicPredicate<T>`] — the universal base. Composes via `&`, `|`,
//!   `^`, `!` operators. Lowers cleanly to SQL when consumed by an ORM
//!   that knows the `Field<T, V>` shape (e.g., djogi). Evaluates
//!   identically against an in-memory `&T` via [`BasicPredicate::evaluate`].
//! - `MemQ<T>` (future extension) — the in-memory-only
//!   extension. Adds closure predicates and trait-impl predicates that
//!   can't be projected into SQL.
//!
//! This module owns the universal base; `MemQ` lives next to the
//! `Punnu` query handle since it's only useful in conjunction with an
//! in-memory pool.

pub mod basic;
pub mod field_ext;
pub mod field_predicate;

pub use basic::BasicPredicate;
pub use field_predicate::{FieldPredicate, LookupOp};
