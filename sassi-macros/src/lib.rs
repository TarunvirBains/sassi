//! # sassi-macros
//!
//! Proc macros for sassi: `#[derive(Cacheable)]` and
//! `#[sassi::trait_impl]`.
//!
//! Macros call into `sassi-codegen` for the actual `TokenStream`
//! emission so the codegen logic stays in a regular library crate
//! that downstream macro crates (e.g., `djogi-macros`) can also
//! consume without running into proc-macro-cycle limitations.

#![forbid(unsafe_code)]

mod cacheable;
mod trait_impl;

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};

/// Derive macro for `sassi::Cacheable`.
///
/// Generates:
/// 1. A companion `{StructName}Fields` struct with one
///    `sassi::Field<Self, FieldType>` per declared field.
/// 2. `impl sassi::Cacheable for {StructName}` with:
///    - `Id` = the type of the field literally named `id`.
///    - `fields()` trait method wiring every accessor to its real
///      extractor (so generic `T: Cacheable` callers can construct
///      wired Fields without knowing the concrete type).
/// 3. When `#[cacheable(watermark_field = "...")]` is present, an
///    `impl sassi::DeltaSyncCacheable` whose `Watermark` is the named
///    field's type and whose `watermark()` clones that field.
///
/// Requirements:
/// - Input must be a struct with named fields.
/// - One of the fields must be literally named `id`.
/// - `id`'s type must implement `Hash + Eq + Clone + Ord + Send + Sync + 'static`.
/// - `watermark_field`, when present, must name a field whose type
///   implements `sassi::MonotonicWatermark`.
#[proc_macro_derive(Cacheable, attributes(cacheable))]
pub fn derive_cacheable(input: TokenStream) -> TokenStream {
    cacheable::derive_cacheable(input)
}

/// Attribute macro for registering a trait implementation with
/// `Sassi::all_impl::<dyn Trait>()`.
///
/// Apply it to a concrete trait impl:
///
/// ```ignore
/// #[sassi::trait_impl]
/// impl Nameable for User {
///     fn name(&self) -> &str { &self.name }
/// }
/// ```
#[proc_macro_attribute]
pub fn trait_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    trait_impl::trait_impl(args, input)
}

fn sassi_path() -> Result<TokenStream2, syn::Error> {
    let found = crate_name("sassi").map_err(|e| {
        syn::Error::new(
            Span::call_site(),
            format!("sassi macro expansion could not resolve the `sassi` crate: {e}"),
        )
    })?;

    Ok(match found {
        FoundCrate::Itself => quote!(crate),
        FoundCrate::Name(name) => {
            let ident = format_ident!("{}", name);
            quote!(::#ident)
        }
    })
}
