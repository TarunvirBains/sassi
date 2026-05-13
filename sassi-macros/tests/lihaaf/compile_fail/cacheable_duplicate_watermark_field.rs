//! `watermark_field` can only be specified once.

use sassi::Cacheable;

#[derive(Cacheable)]
#[cacheable(watermark_field = "updated_at", watermark_field = "version")]
struct DuplicateWatermarkField {
    id: i64,
    updated_at: i64,
    version: i64,
}

fn main() {}
