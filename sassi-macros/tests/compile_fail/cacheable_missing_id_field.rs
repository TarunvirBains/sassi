//! `#[derive(Cacheable)]` requires a field literally named `id`.

use sassi::Cacheable;

#[derive(Cacheable)]
struct NoIdHere {
    name: String,
    age: u32,
}

fn main() {}
