//! Plain `#[derive(Cacheable)]` must not opt a model into delta sync.

use sassi::{Cacheable, DeltaSyncCacheable};

#[derive(Cacheable)]
struct NoWatermarkAttr {
    id: i64,
    updated_at: i64,
}

fn assert_delta_sync<T: DeltaSyncCacheable>() {}

fn main() {
    assert_delta_sync::<NoWatermarkAttr>();
}
