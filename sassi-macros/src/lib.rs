//! # sassi-macros
//!
//! Proc macros for sassi: `#[derive(Cacheable)]` (more on the way:
//! `#[sassi::trait_impl(...)]` lands in a later task).
//!
//! Macros call into `sassi-codegen` for the actual `TokenStream`
//! emission so the codegen logic stays in a regular library crate
//! that downstream macro crates (e.g., `djogi-macros`) can also
//! consume without running into proc-macro-cycle limitations.

#![forbid(unsafe_code)]

mod cacheable;

use proc_macro::TokenStream;

/// Derive macro for `sassi::Cacheable`.
///
/// Generates:
/// 1. A companion `{StructName}Fields` struct with one
///    `sassi::Field<Self, FieldType>` per declared field.
/// 2. `impl sassi::Cacheable for {StructName}` with `Id` = the type of
///    the field literally named `id` (v0.2 will accept
///    `#[cacheable(id)]` on a custom-named field).
/// 3. An inherent `{StructName}::fields()` constructor that wires every
///    accessor to its real extractor.
///
/// Requirements:
/// - Input must be a struct with named fields.
/// - One of the fields must be literally named `id`.
/// - `id`'s type must implement `Hash + Eq + Clone + Ord + Send + Sync + 'static`.
#[proc_macro_derive(Cacheable)]
pub fn derive_cacheable(input: TokenStream) -> TokenStream {
    cacheable::derive_cacheable(input)
}
