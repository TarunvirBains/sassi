//! Attribute macro for registering cross-type trait implementations.
//!
//! The macro supports only `#[sassi::trait_impl] impl Trait for Type`
//! blocks. Expansion emits the original impl plus one
//! `inventory::submit!` block. The `inventory` crate handles the
//! platform-specific startup registration (`.init_array` /
//! `__DATA,__mod_init_func` / `.CRT$XCU` on native, the wasm32
//! initializer on `wasm32-unknown-unknown`), so adopter crates with
//! `#![forbid(unsafe_code)]` are not rejected — the `unsafe`
//! attribute syntax never appears in the macro's output.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{ItemImpl, parse_macro_input, spanned::Spanned};

/// Expand `#[sassi::trait_impl]` on a concrete trait impl.
///
/// The original impl is emitted unchanged. A `inventory::submit!`
/// block registers the `(model type, trait)` pair with sassi's
/// hidden registry so `Sassi::all_impl::<dyn Trait>()` can collect
/// matching entries later.
///
/// # Adopter constraints
///
/// - The impl block must be concrete — generic parameters, `where`
///   clauses, and negative impls are rejected at expansion time
///   with a span-pointing diagnostic.
/// - The trait must satisfy `Send + Sync + 'static`. Without those
///   bounds, the macro's emitted collector cannot box the typed
///   `Vec<Arc<dyn Trait>>` as `Box<dyn Any + Send + Sync>` and the
///   adopter sees a compile-time error at the macro invocation.
///   `'static` is implied by `dyn Trait: Any` whenever the trait
///   has no captured lifetimes; in practice almost every cross-type
///   trait satisfies this naturally.
pub fn trait_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = TokenStream2::from(args);
    let item = parse_macro_input!(input as ItemImpl);

    if !args.is_empty() {
        return syn::Error::new(args.span(), "sassi::trait_impl takes no arguments")
            .to_compile_error()
            .into();
    }

    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return syn::Error::new(
            item.generics.span(),
            "#[sassi::trait_impl] currently supports concrete, non-generic impl blocks",
        )
        .to_compile_error()
        .into();
    }

    let trait_path = match &item.trait_ {
        Some((None, path, _for_token)) => path.clone(),
        Some((Some(not_token), _path, _for_token)) => {
            return syn::Error::new(
                not_token.span(),
                "#[sassi::trait_impl] cannot register negative impl blocks",
            )
            .to_compile_error()
            .into();
        }
        None => {
            return syn::Error::new(
                item.impl_token.span(),
                "sassi::trait_impl must be applied to `impl Trait for Type`",
            )
            .to_compile_error()
            .into();
        }
    };

    let model_ty = &item.self_ty;
    let expanded = quote! {
        #item

        const _: () = {
            fn __sassi_collect_trait_impl(
                sassi: &::sassi::Sassi,
            ) -> ::std::boxed::Box<dyn ::std::any::Any + ::std::marker::Send + ::std::marker::Sync> {
                let values: ::std::vec::Vec<::std::sync::Arc<dyn #trait_path>> =
                    match sassi.pool::<#model_ty>() {
                        Some(pool) => pool
                            .scope(::std::vec::Vec::new())
                            .collect()
                            .into_iter()
                            .map(|value| value as ::std::sync::Arc<dyn #trait_path>)
                            .collect(),
                        None => ::std::vec::Vec::new(),
                    };
                ::std::boxed::Box::new(values)
            }

            ::sassi::__private::inventory::submit! {
                ::sassi::__private::TraitImplEntry {
                    trait_type_id: ::std::any::TypeId::of::<dyn #trait_path>(),
                    model_type_id: ::std::any::TypeId::of::<#model_ty>(),
                    collect_fn: __sassi_collect_trait_impl,
                }
            }
        };
    };

    expanded.into()
}
