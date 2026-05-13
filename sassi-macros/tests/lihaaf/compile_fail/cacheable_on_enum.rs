//! `#[derive(Cacheable)]` only supports structs (no enums or unions).

use sassi::Cacheable;

#[derive(Cacheable)]
enum NotAStruct {
    A,
    B,
}

fn main() {}
