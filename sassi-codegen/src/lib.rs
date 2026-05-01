//! # sassi-codegen
//!
//! Shared codegen primitives for `sassi-macros` and downstream
//! proc-macro consumers (e.g., `djogi-macros`).
//!
//! Proc-macro crates can't depend on each other directly, but they can
//! share a regular library crate. `sassi-codegen` is that library: it
//! emits `TokenStream`s for `Cacheable` derive output (the companion
//! `{Name}Fields` struct + the `Cacheable` impl + the `T::fields()`
//! constructor). Each entry point takes a `sassi_path: &TokenStream`
//! parameter so the caller can target whatever path prefix the
//! end-user crate exposes (`::sassi` from `sassi-macros`,
//! `::djogi::cache` from a future `djogi-macros` integration).
//!
//! Consumers of this crate build their proc-macro by:
//! 1. Parsing the input via `syn::parse_macro_input!(input as DeriveInput)`.
//! 2. Calling [`generate_fields_struct`] + [`generate_cacheable_impl`].
//! 3. Combining the resulting `TokenStream`s and returning.
//!
//! See `sassi-macros/src/cacheable.rs` for the canonical example.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod cacheable_impl;
mod fields_struct;

pub use cacheable_impl::generate_cacheable_impl;
pub use fields_struct::generate_fields_struct;
