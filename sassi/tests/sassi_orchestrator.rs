//! Integration tests for [`sassi::Sassi`] cross-type trait queries.

use sassi::{Cacheable, Punnu, Sassi};
use std::any::Any;
use std::future::Future;
use std::sync::Arc;

trait Nameable: Send + Sync + Any {
    fn name(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Clone)]
struct Foo {
    id: i64,
    name: String,
}

#[derive(Debug, Clone)]
struct Bar {
    id: i64,
    name: String,
}

#[derive(Default)]
struct EmptyFields;

impl Cacheable for Foo {
    type Id = i64;
    type Fields = EmptyFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> EmptyFields {
        EmptyFields
    }
}

impl Cacheable for Bar {
    type Id = i64;
    type Fields = EmptyFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> EmptyFields {
        EmptyFields
    }
}

#[sassi::trait_impl]
impl Nameable for Foo {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[sassi::trait_impl]
impl Nameable for Bar {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime should build")
        .block_on(future)
}

#[test]
fn all_impl_returns_trait_objects_from_registered_pools() {
    let mut sassi = Sassi::new();
    let foos = Arc::new(Punnu::<Foo>::builder().build());
    let bars = Arc::new(Punnu::<Bar>::builder().build());
    sassi.register::<Foo>(foos.clone());
    sassi.register::<Bar>(bars.clone());

    block_on(async {
        foos.insert(Foo {
            id: 1,
            name: "alpha".to_owned(),
        })
        .await
        .unwrap();
        bars.insert(Bar {
            id: 10,
            name: "bravo".to_owned(),
        })
        .await
        .unwrap();
    });

    let items: Vec<Arc<dyn Nameable>> = sassi.all_impl::<dyn Nameable>();
    let mut names = items
        .iter()
        .map(|item| item.name().to_owned())
        .collect::<Vec<_>>();
    names.sort();

    assert_eq!(names, vec!["alpha", "bravo"]);
    assert!(
        items
            .iter()
            .any(|item| item.as_any().downcast_ref::<Foo>().is_some())
    );
    assert!(
        items
            .iter()
            .any(|item| item.as_any().downcast_ref::<Bar>().is_some())
    );
}

#[test]
fn pool_returns_the_registered_arc_for_a_type() {
    let mut sassi = Sassi::new();
    let foos = Arc::new(Punnu::<Foo>::builder().build());

    sassi.register::<Foo>(foos.clone());

    let retrieved = sassi.pool::<Foo>().expect("Foo pool should be registered");
    assert!(Arc::ptr_eq(&retrieved, &foos));
}

#[test]
fn re_registering_same_type_replaces_previous_pool() {
    let mut sassi = Sassi::new();
    let first = Arc::new(Punnu::<Foo>::builder().build());
    let second = Arc::new(Punnu::<Foo>::builder().build());

    block_on(async {
        first
            .insert(Foo {
                id: 1,
                name: "old".to_owned(),
            })
            .await
            .unwrap();
        second
            .insert(Foo {
                id: 2,
                name: "new".to_owned(),
            })
            .await
            .unwrap();
    });

    sassi.register::<Foo>(first.clone());
    sassi.register::<Foo>(second.clone());

    let retrieved = sassi.pool::<Foo>().expect("Foo pool should be registered");
    assert!(Arc::ptr_eq(&retrieved, &second));

    let names = sassi
        .all_impl::<dyn Nameable>()
        .iter()
        .map(|item| item.name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec!["new"],
        "Sassi owns one pool per concrete T; re-registration replaces, not isolates tenants"
    );
}
