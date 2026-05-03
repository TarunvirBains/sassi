//! Parsing support for `#[derive(Cacheable)]` helper attributes.

use proc_macro2::Span;
use syn::{Data, DeriveInput, Fields, LitStr};

/// Parsed options from `#[cacheable(...)]` helper attributes.
#[derive(Debug, Default)]
pub struct CacheableDeriveOptions {
    /// Optional field that should back `DeltaSyncCacheable::watermark`.
    pub watermark_field: Option<WatermarkField>,
}

/// Name and source span for a requested watermark field.
#[derive(Debug)]
pub struct WatermarkField {
    /// Field name provided in `#[cacheable(watermark_field = "...")]`.
    pub name: String,
    /// Span of the string literal naming the field.
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

/// Parse `#[cacheable(...)]` helper attributes on a derive input.
pub fn parse_cacheable_derive_options(
    input: &DeriveInput,
) -> Result<CacheableDeriveOptions, syn::Error> {
    reject_field_level_cacheable_attrs(input)?;

    let mut options = CacheableDeriveOptions::default();

    for attr in &input.attrs {
        if !attr.path().is_ident("cacheable") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("watermark_field") {
                return Err(meta.error("Cacheable: unsupported #[cacheable(...)] option"));
            }

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
            Ok(())
        })?;
    }

    Ok(options)
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
                     place `watermark_field` on the struct",
                ));
            }
        }
    }

    Ok(())
}
