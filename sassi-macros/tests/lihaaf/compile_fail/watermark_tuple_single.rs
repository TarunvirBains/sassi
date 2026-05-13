//! One-element tuples are not accepted as watermark cursors.

use sassi::MonotonicWatermark;

fn assert_watermark<T: MonotonicWatermark>() {}

fn main() {
    assert_watermark::<(i64,)>();
}
