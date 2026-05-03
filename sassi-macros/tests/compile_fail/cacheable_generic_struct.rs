//! `#[derive(Cacheable)]` should reject generic structs with a clear
//! diagnostic until generic field companion structs are supported.

use sassi::Cacheable;

#[derive(Cacheable)]
struct GenericEntity<T> {
    id: i64,
    value: T,
}

fn main() {}
