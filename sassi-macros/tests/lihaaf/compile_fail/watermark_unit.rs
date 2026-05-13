//! Unit is not a monotonic watermark type.

use sassi::MonotonicWatermark;

fn assert_watermark<T: MonotonicWatermark>() {}

fn main() {
    assert_watermark::<()>();
}
