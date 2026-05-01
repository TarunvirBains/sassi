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
//! See `docs/superpowers/specs/2026-04-30-sassi-design.md` for the
//! full design (local; will be promoted to `docs/spec/` when finalized).
//!
//! Pre-v0.1.0; this crate is currently a skeleton. Implementation lands
//! per the linked design spec; see the corresponding repository's
//! implementation plan for sequencing.

#![forbid(unsafe_code)]

/// Workspace placeholder. Real surface lands per the design spec.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
