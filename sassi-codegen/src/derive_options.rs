//! Parsing support for `#[derive(Cacheable)]` helper attributes.

use proc_macro2::{Span, TokenStream};
use syn::{Data, DeriveInput, Fields, LitStr, Token, spanned::Spanned};

/// Parsed options from `#[cacheable(...)]` helper attributes.
#[derive(Debug, Default)]
pub struct CacheableDeriveOptions {
    /// Optional field that should back `DeltaSyncCacheable::watermark`.
    pub watermark_field: Option<WatermarkField>,
    /// Optional stable L2 backend keyspace type name.
    pub type_name: Option<CacheTypeName>,
    /// Optional postcard wire portability guard.
    pub wire_portable: Option<WirePortableOption>,
    /// Companion field surface used for `Cacheable::Fields`.
    pub fields: CacheableFieldsMode,
}

/// Companion field-surface mode for generated `Cacheable` impls.
#[derive(Debug, Default)]
pub enum CacheableFieldsMode {
    /// Use sassi-codegen's generated `{Model}Fields` companion and construct
    /// one `sassi::Field<Model, V>` accessor per model field.
    #[default]
    Generated,
    /// Reference a consumer-owned companion type and constructor expression.
    ///
    /// Downstream macro crates use this when they already generate their own
    /// `{Model}Fields` type and need only the `Cacheable` impl from
    /// sassi-codegen.
    External {
        /// Token path for `type Fields = ...`.
        type_path: TokenStream,
        /// Expression used by `fn fields() -> Self::Fields`.
        constructor: TokenStream,
    },
}

/// Name and source span for a requested watermark field.
#[derive(Debug)]
pub struct WatermarkField {
    /// Field name provided in `#[cacheable(watermark_field = "...")]`.
    pub name: String,
    /// Span of the string literal naming the field.
    pub span: Span,
}

/// Stable backend keyspace type name requested by `#[cacheable(type_name = "...")]`.
#[derive(Debug)]
pub struct CacheTypeName {
    /// Application-owned stable cache type name.
    pub value: String,
    /// Span of the string literal naming the type.
    pub span: Span,
}

/// Source span for `#[cacheable(wire_portable)]`.
#[derive(Debug)]
pub struct WirePortableOption {
    /// Span of the bare `wire_portable` option.
    pub span: Span,
}

impl WatermarkField {
    /// Construct a watermark-field option from a field name and source span.
    ///
    /// Downstream macro crates with their own attribute grammar can use
    /// this to feed [`generate_delta_sync_cacheable_impl`](crate::generate_delta_sync_cacheable_impl)
    /// without depending on sassi's exact `#[cacheable(...)]` syntax.
    pub fn new(name: impl Into<String>, span: Span) -> Self {
        Self {
            name: name.into(),
            span,
        }
    }
}

impl CacheTypeName {
    /// Construct a stable cache type-name option from a value and source span.
    ///
    /// Downstream macro crates with their own attribute grammar can use this to
    /// feed [`generate_cacheable_impl`](crate::generate_cacheable_impl)
    /// without depending on sassi's exact `#[cacheable(...)]` syntax.
    pub fn new(value: impl Into<String>, span: Span) -> Self {
        Self {
            value: value.into(),
            span,
        }
    }
}

impl WirePortableOption {
    /// Construct a wire-portable option from its source span.
    pub fn new(span: Span) -> Self {
        Self { span }
    }
}

impl CacheableFieldsMode {
    /// Construct an external field companion mode.
    ///
    /// `type_path` is emitted after `type Fields =`; `constructor` is emitted
    /// as the body expression of `fn fields() -> Self::Fields`.
    pub fn external(
        type_path: impl Into<TokenStream>,
        constructor: impl Into<TokenStream>,
    ) -> Self {
        Self::External {
            type_path: type_path.into(),
            constructor: constructor.into(),
        }
    }
}

/// Parse `#[cacheable(...)]` helper attributes on a derive input.
pub fn parse_cacheable_derive_options(
    input: &DeriveInput,
) -> Result<CacheableDeriveOptions, syn::Error> {
    reject_generic_cacheable_input(input)?;
    reject_field_level_cacheable_attrs(input)?;

    let mut options = CacheableDeriveOptions::default();

    for attr in &input.attrs {
        if !attr.path().is_ident("cacheable") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("watermark_field") {
                if options.watermark_field.is_some() {
                    return Err(meta.error("Cacheable: duplicate `watermark_field` option"));
                }

                let value = meta.value()?;
                let field: LitStr = value.parse()?;
                let name = field.value();
                if name.is_empty() {
                    return Err(syn::Error::new(
                        field.span(),
                        "Cacheable: `watermark_field` cannot be empty",
                    ));
                }

                options.watermark_field = Some(WatermarkField::new(name, field.span()));
                return Ok(());
            }

            if meta.path.is_ident("type_name") {
                if options.type_name.is_some() {
                    return Err(meta.error("Cacheable: duplicate `type_name` option"));
                }

                let value = meta.value()?;
                let type_name: LitStr = value.parse()?;
                let name = type_name.value();
                if name.is_empty() {
                    return Err(syn::Error::new(
                        type_name.span(),
                        "Cacheable: `type_name` cannot be empty",
                    ));
                }

                options.type_name = Some(CacheTypeName::new(name, type_name.span()));
                return Ok(());
            }

            if meta.path.is_ident("wire_portable") {
                if options.wire_portable.is_some() {
                    return Err(meta.error("Cacheable: duplicate `wire_portable` option"));
                }
                if meta.input.peek(Token![=]) {
                    return Err(meta.error("Cacheable: `wire_portable` does not take a value"));
                }
                if meta.input.peek(syn::token::Paren) {
                    return Err(meta.error("Cacheable: `wire_portable` does not take arguments"));
                }

                options.wire_portable = Some(WirePortableOption::new(meta.path.span()));
                return Ok(());
            }

            Err(meta.error("Cacheable: unsupported #[cacheable(...)] option"))
        })?;
    }

    Ok(options)
}

fn reject_generic_cacheable_input(input: &DeriveInput) -> Result<(), syn::Error> {
    if input.generics.params.is_empty() && input.generics.where_clause.is_none() {
        return Ok(());
    }

    Err(syn::Error::new(
        input.generics.span(),
        "Cacheable: generic structs are not supported in v0.1; use a concrete wrapper type",
    ))
}

fn reject_field_level_cacheable_attrs(input: &DeriveInput) -> Result<(), syn::Error> {
    let Data::Struct(s) = &input.data else {
        return Ok(());
    };

    let Fields::Named(named) = &s.fields else {
        return Ok(());
    };

    for field in &named.named {
        for attr in &field.attrs {
            if attr.path().is_ident("cacheable") {
                return Err(syn::Error::new_spanned(
                    attr,
                    "Cacheable: field-level #[cacheable(...)] options are not supported; \
                     place cacheable options on the struct",
                ));
            }
        }
    }

    Ok(())
}
