//! Unknown `#[cacheable(...)]` options are rejected.

use sassi::Cacheable;

#[derive(Cacheable)]
#[cacheable(foo = "bar")]
struct UnknownCacheableOption {
    id: i64,
}

fn main() {}
