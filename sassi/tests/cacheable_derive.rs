//! End-to-end verification of `#[derive(Cacheable)]`. Confirms the
//! macro emits the companion `Fields` struct, the `Cacheable` trait
//! impl, and the `T::fields()` constructor — and that they all
//! compose with [`BasicPredicate`] / `Field` lookup methods from the
//! preceding tasks.

use sassi::{BasicPredicate, Cacheable};

#[derive(Cacheable, Debug, Clone)]
struct Item {
    id: i64,
    name: String,
    price: u32,
    on_sale: bool,
}

#[derive(Cacheable, Debug, Clone)]
#[cacheable(type_name = "sassi.test.StableDerivedItem")]
struct StableDerivedItem {
    id: i64,
}

#[test]
fn derive_emits_cacheable_impl() {
    let item = Item {
        id: 42,
        name: "Widget".into(),
        price: 1999,
        on_sale: true,
    };
    // `Cacheable::id` is reachable via the trait method.
    assert_eq!(<Item as Cacheable>::id(&item), 42);
}

#[test]
fn derive_can_emit_stable_backend_type_name() {
    assert_eq!(
        <StableDerivedItem as Cacheable>::cache_type_name(),
        "sassi.test.StableDerivedItem"
    );
}

fn first_local_row_type_name() -> &'static str {
    #[derive(Cacheable, Debug, Clone)]
    struct Row {
        id: i64,
    }

    <Row as Cacheable>::cache_type_name()
}

fn second_local_row_type_name() -> &'static str {
    #[derive(Cacheable, Debug, Clone)]
    struct Row {
        id: i64,
    }

    <Row as Cacheable>::cache_type_name()
}

#[test]
fn derive_default_cache_type_name_distinguishes_local_same_named_structs() {
    assert_ne!(first_local_row_type_name(), second_local_row_type_name());
}

#[test]
fn derive_emits_fields_constructor_with_real_extractors() {
    let f = Item::fields();
    let item = Item {
        id: 7,
        name: "Gadget".into(),
        price: 999,
        on_sale: false,
    };

    // Every accessor's `extract` method returns a reference to the matching field.
    assert_eq!(*f.id.extract(&item), 7);
    assert_eq!(f.name.extract(&item), "Gadget");
    assert_eq!(*f.price.extract(&item), 999);
    assert!(!*f.on_sale.extract(&item));

    // Every accessor's `name()` matches the field identifier.
    assert_eq!(f.id.name(), "id");
    assert_eq!(f.name.name(), "name");
    assert_eq!(f.price.name(), "price");
    assert_eq!(f.on_sale.name(), "on_sale");
}

#[test]
fn derive_composes_with_predicate_algebra() {
    let f = Item::fields();
    let alice = Item {
        id: 1,
        name: "Alice's pick".into(),
        price: 500,
        on_sale: true,
    };
    let bob = Item {
        id: 2,
        name: "Bob's pick".into(),
        price: 1500,
        on_sale: false,
    };

    let cheap_or_sale: BasicPredicate<Item> = f.price.lt(1000) | f.on_sale.eq(true);
    assert!(cheap_or_sale.evaluate(&alice));
    assert!(!cheap_or_sale.evaluate(&bob));

    let names_match: BasicPredicate<Item> = f.name.icontains("ALICE");
    assert!(names_match.evaluate(&alice));
    assert!(!names_match.evaluate(&bob));
}

#[test]
fn derive_default_fields_returns_unwired_extractors() {
    // `T::Fields::default()` works because `Field<T, V>: Default` —
    // but the extractors panic if invoked.
    let _f = ItemFields::default();
    // No assertion — just verifying it compiles + constructs without
    // panicking. Extractor invocation would panic.
}
