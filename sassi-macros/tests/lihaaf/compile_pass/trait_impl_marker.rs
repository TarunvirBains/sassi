use sassi::{Cacheable, TraitImpl};
use std::sync::Arc;

trait Searchable: Send + Sync {
    fn cols(&self) -> &'static [&'static str];
}

#[derive(Debug, Clone)]
struct Vehicle { id: i64 }

#[derive(Default)]
struct EmptyFields;

impl Cacheable for Vehicle {
    type Id = i64;
    type Fields = EmptyFields;
    fn id(&self) -> i64 { self.id }
    fn fields() -> EmptyFields { EmptyFields }
}

#[sassi::trait_impl]
impl Searchable for Vehicle {
    fn cols(&self) -> &'static [&'static str] { &["title"] }
}

fn requires_marker<T: TraitImpl<dyn Searchable>>() {}

fn main() {
    requires_marker::<Vehicle>();
    let _ = Arc::new(Vehicle { id: 1 });
}
