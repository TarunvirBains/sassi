//! # sassi-macros
//!
//! Proc macros for sassi: `#[derive(Cacheable)]` + `#[sassi::trait_impl]`.
//!
//! Macros call into `sassi-codegen` for the actual `TokenStream`
//! emission so the codegen logic stays in a regular library crate
//! that downstream macro crates (e.g., `djogi-macros`) can also
//! consume.
//!
//! Pre-v0.1.0 skeleton.

#![forbid(unsafe_code)]
