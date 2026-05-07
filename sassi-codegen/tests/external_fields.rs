use quote::quote;
use sassi_codegen::{
    CacheableDeriveOptions, CacheableFieldsMode, generate_cacheable_impl, generate_fields_struct,
};
use syn::{DeriveInput, parse_quote};

#[test]
fn generated_mode_still_emits_standard_fields_constructor() {
    let input: DeriveInput = parse_quote! {
        struct User {
            id: i64,
            name: String,
        }
    };

    let fields = generate_fields_struct(&input, &quote!(::sassi)).unwrap();
    let cacheable =
        generate_cacheable_impl(&input, &CacheableDeriveOptions::default(), &quote!(::sassi))
            .unwrap();

    let fields = fields.to_string();
    let cacheable = cacheable.to_string();

    assert!(fields.contains("struct UserFields"));
    assert!(cacheable.contains("type Fields = UserFields"));
    assert!(cacheable.contains("name : :: sassi :: Field :: new"));
}

#[test]
fn external_fields_mode_uses_consumer_owned_type_and_constructor() {
    let input: DeriveInput = parse_quote! {
        struct User {
            id: i64,
            name: String,
        }
    };
    let options = CacheableDeriveOptions {
        fields: CacheableFieldsMode::external(quote!(UserFields), quote!(UserFields::new())),
        ..Default::default()
    };

    let cacheable = generate_cacheable_impl(&input, &options, &quote!(::sassi))
        .unwrap()
        .to_string();

    assert!(cacheable.contains("type Fields = UserFields"));
    assert!(cacheable.contains("fn fields () -> Self :: Fields { UserFields :: new () }"));
    assert!(!cacheable.contains("name : :: sassi :: Field :: new"));
}
