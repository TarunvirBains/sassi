//! Tuple watermark components must all be monotonic watermark types.

use sassi::MonotonicWatermark;

fn assert_watermark<T: MonotonicWatermark>() {}

fn main() {
    assert_watermark::<(i64, bool)>();
}
