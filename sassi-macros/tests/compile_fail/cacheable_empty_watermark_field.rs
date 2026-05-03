//! `watermark_field` must name a field.

use sassi::Cacheable;

#[derive(Cacheable)]
#[cacheable(watermark_field = "")]
struct EmptyWatermarkField {
    id: i64,
    updated_at: i64,
}

fn main() {}
