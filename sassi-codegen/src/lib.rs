//! # sassi-codegen
//!
//! Support crate with codegen primitives for `sassi-macros` and downstream
//! proc-macro consumers.
//!
//! Ordinary adopters depend on `sassi`, not this crate directly.
//!
//! Proc-macro crates can't depend on each other directly, but they can
//! share a regular library crate. `sassi-codegen` is that library: it
//! emits `TokenStream`s for `Cacheable` derive output (the companion
//! `{Name}Fields` struct, the `Cacheable` impl, stable backend type-name
//! support, the `T::fields()` constructor, optional external field-companion
//! routing for downstream macro crates, and optional `DeltaSyncCacheable`
//! impls). Each entry
//! point takes a `sassi_path: &TokenStream` parameter so the caller can
//! target whatever path prefix the end-user crate exposes (`::sassi`
//! from `sassi-macros`, or an aliased path from a downstream macro
//! crate).
//!
//! Consumers of this crate build their proc-macro by:
//! 1. Parsing the input via `syn::parse_macro_input!(input as DeriveInput)`.
//! 2. Calling [`parse_cacheable_derive_options`].
//! 3. Calling [`generate_fields_struct`], [`generate_cacheable_impl`],
//!    and [`generate_delta_sync_cacheable_impl`].
//! 4. Combining the resulting `TokenStream`s and returning.
//!
//! See `sassi-macros/src/cacheable.rs` for the canonical example.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod cacheable_impl;
mod derive_options;
mod fields_struct;

pub use cacheable_impl::{
    generate_cacheable_impl, generate_delta_sync_cacheable_impl, generate_wire_portable_impl,
};
pub use derive_options::{
    CacheTypeName, CacheableDeriveOptions, CacheableFieldsMode, WatermarkField, WirePortableOption,
    parse_cacheable_derive_options,
};
pub use fields_struct::generate_fields_struct;

/// Convert a `FoundCrate` result from `proc-macro-crate::crate_name("sassi")`
/// into the absolute path token stream that codegen functions should use as
/// the `sassi_path` prefix.
///
/// - `FoundCrate::Itself` → `::sassi`
///   (the calling macro is being expanded inside the `sassi` crate itself)
/// - `FoundCrate::Name(name)` → `::<name>`
///   (the adopter renamed the `sassi` dependency in their `Cargo.toml`)
///
/// This function is the single, testable source of truth for the path-resolution
/// logic shared by `sassi-macros` and any downstream macro crate that
/// consumes `sassi-codegen`.
pub fn resolve_sassi_path(found: proc_macro_crate::FoundCrate) -> proc_macro2::TokenStream {
    use proc_macro_crate::FoundCrate;
    use quote::{format_ident, quote};

    match found {
        FoundCrate::Itself => quote!(::sassi),
        FoundCrate::Name(name) => {
            let ident = format_ident!("{}", name);
            quote!(::#ident)
        }
    }
}
