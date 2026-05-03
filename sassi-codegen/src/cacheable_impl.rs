//! [`generate_cacheable_impl`] — emits `impl Cacheable for T`, including
//! the `id()` extractor and the `fields()` constructor wired to real
//! extractors. Both methods are required by the trait so generic code
//! over `T: Cacheable` can call them without knowing the concrete type.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Field, Fields};

use crate::derive_options::CacheableDeriveOptions;

/// Emit `impl Cacheable for T`:
///
/// 1. `type Id` — set to the type of the field literally named `id`.
/// 2. `type Fields` — set to `{Name}Fields` (companion struct produced
///    by [`generate_fields_struct`](super::fields_struct::generate_fields_struct)).
/// 3. `fn id(&self) -> Self::Id` — clones `self.id`.
/// 4. `fn fields() -> Self::Fields` — constructs the companion with
///    every accessor wired to its real extractor. Required as a trait
///    method (rather than inherent) so generic code can call
///    `T::fields()` without knowing the concrete type.
pub fn generate_cacheable_impl(
    input: &DeriveInput,
    sassi_path: &TokenStream,
) -> Result<TokenStream, syn::Error> {
    let struct_name = &input.ident;
    let fields_name = format_ident!("{}Fields", struct_name);

    let named = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "Cacheable: only named-field structs are supported",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "Cacheable: only structs are supported (no enums or unions)",
            ));
        }
    };

    let id_field = find_id_field(named, struct_name)?;
    let id_ty = &id_field.ty;

    // For each declared field, emit `name: Field::new("name", |s| &s.name)`.
    let field_constructors = named.iter().map(|f| {
        let name = f.ident.as_ref().unwrap();
        let name_str = name.to_string();
        quote! {
            #name: #sassi_path::Field::new(#name_str, |s| &s.#name)
        }
    });

    Ok(quote! {
        impl #sassi_path::Cacheable for #struct_name {
            type Id = #id_ty;
            type Fields = #fields_name;

            fn id(&self) -> Self::Id {
                ::core::clone::Clone::clone(&self.id)
            }

            fn fields() -> Self::Fields {
                #fields_name {
                    #(#field_constructors,)*
                }
            }
        }
    })
}

/// Emit `impl DeltaSyncCacheable for T` when the derive input requests
/// `#[cacheable(watermark_field = "...")]`.
///
/// When no watermark field is configured, this returns an empty
/// token stream so ordinary `#[derive(Cacheable)]` users do not opt in
/// to delta-sync contracts accidentally.
pub fn generate_delta_sync_cacheable_impl(
    input: &DeriveInput,
    options: &CacheableDeriveOptions,
    sassi_path: &TokenStream,
) -> Result<TokenStream, syn::Error> {
    let Some(watermark) = &options.watermark_field else {
        return Ok(TokenStream::new());
    };

    let struct_name = &input.ident;
    let named = named_fields(input, struct_name)?;
    let watermark_field = named
        .iter()
        .find(|f| {
            f.ident
                .as_ref()
                .map(|ident| ident == &watermark.name)
                .unwrap_or(false)
        })
        .ok_or_else(|| {
            syn::Error::new(
                watermark.span,
                format!(
                    "Cacheable: watermark_field `{}` does not name a field on `{}`",
                    watermark.name, struct_name
                ),
            )
        })?;

    let watermark_ident = watermark_field.ident.as_ref().unwrap();
    let watermark_ty = &watermark_field.ty;

    Ok(quote! {
        impl #sassi_path::DeltaSyncCacheable for #struct_name {
            type Watermark = #watermark_ty;

            fn watermark(&self) -> Self::Watermark {
                ::core::clone::Clone::clone(&self.#watermark_ident)
            }
        }
    })
}

/// Find the field named literally `id`. v0.2 will accept
/// `#[cacheable(id)]` on a custom-named field; for v0.1 the rule is
/// "your struct has a field called `id`."
fn find_id_field<'a>(
    fields: &'a syn::punctuated::Punctuated<Field, syn::Token![,]>,
    struct_name: &syn::Ident,
) -> Result<&'a Field, syn::Error> {
    fields
        .iter()
        .find(|f| f.ident.as_ref().map(|i| i == "id").unwrap_or(false))
        .ok_or_else(|| {
            syn::Error::new_spanned(
                struct_name,
                "Cacheable: requires a field literally named `id` (v0.2 will support \
                 `#[cacheable(id)]` for custom names)",
            )
        })
}

fn named_fields<'a>(
    input: &'a DeriveInput,
    struct_name: &syn::Ident,
) -> Result<&'a syn::punctuated::Punctuated<Field, syn::Token![,]>, syn::Error> {
    match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => Ok(&named.named),
            _ => Err(syn::Error::new_spanned(
                struct_name,
                "Cacheable: only named-field structs are supported",
            )),
        },
        _ => Err(syn::Error::new_spanned(
            struct_name,
            "Cacheable: only structs are supported (no enums or unions)",
        )),
    }
}
