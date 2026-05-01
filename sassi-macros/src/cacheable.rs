//! `#[derive(Cacheable)]` proc-macro entry point.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

pub fn derive_cacheable(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let sassi_path: TokenStream2 = quote!(::sassi);

    let fields_struct = match sassi_codegen::generate_fields_struct(&parsed, &sassi_path) {
        Ok(ts) => ts,
        Err(e) => return e.to_compile_error().into(),
    };

    let cacheable_impl = match sassi_codegen::generate_cacheable_impl(&parsed, &sassi_path) {
        Ok(ts) => ts,
        Err(e) => return e.to_compile_error().into(),
    };

    let combined: TokenStream2 = quote! {
        #fields_struct
        #cacheable_impl
    };
    combined.into()
}
