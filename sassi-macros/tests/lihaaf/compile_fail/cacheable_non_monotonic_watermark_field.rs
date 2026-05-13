//! The named watermark field must implement `MonotonicWatermark`.

use sassi::Cacheable;

#[derive(Cacheable)]
#[cacheable(watermark_field = "flag")]
struct NonMonotonicWatermark {
    id: i64,
    flag: bool,
}

fn main() {}
