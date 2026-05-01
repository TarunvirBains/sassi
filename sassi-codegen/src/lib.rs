//! # sassi-codegen
//!
//! Shared codegen primitives for `sassi-macros` and downstream
//! proc-macro consumers (e.g., `djogi-macros`). This is a regular
//! library crate, **not** a proc-macro crate — proc-macro crates
//! cannot depend on each other, but they can share a library that
//! emits `TokenStream`s.
//!
//! The drift-prevention story for `Cacheable` derive: both
//! `sassi-macros::Cacheable` and `djogi-macros::Model` (which auto-
//! derives `Cacheable` for djogi models) call into this crate's
//! `generate_fields_struct(...)`, `generate_cacheable_impl(...)`,
//! etc. — keeping the two macro emitters in lockstep without a
//! git submodule.
//!
//! Pre-v0.1.0 skeleton. See `docs/superpowers/specs/...` for the
//! design.

#![forbid(unsafe_code)]
