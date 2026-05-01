//! Attribute macro for registering cross-type trait implementations.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{ItemImpl, Path, parse_macro_input, spanned::Spanned};

/// Expand `#[sassi::trait_impl]` on a concrete trait impl.
///
/// The original impl is emitted unchanged. A small startup
/// constructor registers the `(model type, trait)` pair with sassi's
/// hidden registry so `Sassi::all_impl::<dyn Trait>()` can collect
/// matching entries later.
pub fn trait_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = TokenStream2::from(args);
    let item = parse_macro_input!(input as ItemImpl);

    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return syn::Error::new(
            item.generics.span(),
            "#[sassi::trait_impl] currently supports concrete, non-generic impl blocks",
        )
        .to_compile_error()
        .into();
    }

    let attr_trait = if args.is_empty() {
        None
    } else {
        match syn::parse2::<Path>(args) {
            Ok(path) => Some(path),
            Err(err) => return err.to_compile_error().into(),
        }
    };

    let impl_trait = match &item.trait_ {
        Some((None, path, _for_token)) => Some(path.clone()),
        Some((Some(not_token), _path, _for_token)) => {
            return syn::Error::new(
                not_token.span(),
                "#[sassi::trait_impl] cannot register negative impl blocks",
            )
            .to_compile_error()
            .into();
        }
        None => None,
    };

    let trait_path = match (attr_trait, impl_trait) {
        (None, Some(path)) => path,
        (Some(path), None) => path,
        (Some(_), Some(path)) => {
            return syn::Error::new(
                path.span(),
                "#[sassi::trait_impl] takes no arguments when applied to a trait impl",
            )
            .to_compile_error()
            .into();
        }
        (None, None) => {
            return syn::Error::new(
                item.impl_token.span(),
                "#[sassi::trait_impl] must be applied to `impl Trait for Type` or passed a trait path",
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

            #[used]
            #[cfg_attr(
                any(
                    target_os = "android",
                    target_os = "dragonfly",
                    target_os = "freebsd",
                    target_os = "linux",
                    target_os = "netbsd",
                    target_os = "openbsd"
                ),
                unsafe(link_section = ".init_array")
            )]
            #[cfg_attr(
                any(target_os = "ios", target_os = "macos"),
                unsafe(link_section = "__DATA,__mod_init_func")
            )]
            #[cfg_attr(target_os = "windows", unsafe(link_section = ".CRT$XCU"))]
            static __SASSI_REGISTER_TRAIT_IMPL: extern "C" fn() = {
                extern "C" fn __sassi_register_trait_impl() {
                    ::sassi::__private::register_trait_impl_raw(
                        ::std::any::TypeId::of::<dyn #trait_path>(),
                        ::std::any::TypeId::of::<#model_ty>(),
                        __sassi_collect_trait_impl,
                    );
                }
                __sassi_register_trait_impl
            };
        };
    };

    expanded.into()
}
