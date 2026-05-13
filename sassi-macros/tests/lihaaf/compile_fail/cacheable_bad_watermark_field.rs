//! `#[cacheable(watermark_field = "...")]` must name an existing field.

use sassi::Cacheable;

#[derive(Cacheable)]
#[cacheable(watermark_field = "updated_at")]
struct MissingWatermarkField {
    id: i64,
    name: String,
}

fn main() {}
