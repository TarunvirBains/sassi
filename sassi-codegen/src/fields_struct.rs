//! [`generate_fields_struct`] — emits the companion `{StructName}Fields`
//! struct that pairs each declared struct field with a
//! [`sassi::Field<T, V>`] accessor.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields};

/// Emit `{StructName}Fields` — one accessor per declared field of
/// `input`, plus a `#[derive(Default)]` so callers can construct
/// placeholder accessors via `T::Fields::default()`.
/// Generated accessor fields keep the visibility of the source fields. That
/// means a `pub` model with private fields still derives private accessors; make
/// the source field visible or hand-write `Cacheable` when external crates need
/// direct access to `UserFields::name`.
///
/// `sassi_path` is the path prefix at which the consumer reaches
/// sassi's public types — typically `::sassi` when called from
/// `sassi-macros`, or `::djogi::cache` when called from
/// `djogi-macros`. Parameterising avoids hard-coding the path so this
/// crate stays consumable by any downstream proc-macro that wants to
/// derive `Cacheable` for its own model types.
pub fn generate_fields_struct(
    input: &DeriveInput,
    sassi_path: &TokenStream,
) -> Result<TokenStream, syn::Error> {
    let struct_name = &input.ident;
    let fields_name = format_ident!("{}Fields", struct_name);
    let vis = &input.vis;

    let named = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => named,
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

    let field_decls = named.named.iter().map(|f| {
        let name = f.ident.as_ref().unwrap();
        let ty = &f.ty;
        let field_vis = &f.vis;
        quote! { #field_vis #name: #sassi_path::Field<#struct_name, #ty> }
    });

    Ok(quote! {
        #[derive(Default)]
        #vis struct #fields_name {
            #(#field_decls,)*
        }
    })
}
