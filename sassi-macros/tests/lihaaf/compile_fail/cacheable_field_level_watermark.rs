//! `watermark_field` belongs on the struct, not on a field.

use sassi::Cacheable;

#[derive(Cacheable)]
struct FieldLevelWatermark {
    id: i64,
    #[cacheable(watermark_field = "updated_at")]
    updated_at: i64,
}

fn main() {}
