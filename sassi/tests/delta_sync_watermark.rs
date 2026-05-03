//! Watermark substrate coverage for delta-sync cacheable models.

use sassi::{Cacheable, DeltaSyncCacheable, MonotonicWatermark};

#[derive(Cacheable, Debug, Clone)]
#[cacheable(watermark_field = "updated_at")]
struct WatermarkedItem {
    id: i64,
    updated_at: i64,
    shard: u64,
}

#[derive(Debug, Clone)]
struct ManualWatermarkedItem {
    id: i64,
    version: (i64, u64),
}

#[derive(Default)]
struct ManualWatermarkedItemFields {
    id: sassi::Field<ManualWatermarkedItem, i64>,
    version: sassi::Field<ManualWatermarkedItem, (i64, u64)>,
}

impl Cacheable for ManualWatermarkedItem {
    type Id = i64;
    type Fields = ManualWatermarkedItemFields;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {
        ManualWatermarkedItemFields {
            id: sassi::Field::new("id", |item| &item.id),
            version: sassi::Field::new("version", |item| &item.version),
        }
    }
}

impl DeltaSyncCacheable for ManualWatermarkedItem {
    type Watermark = (i64, u64);

    fn watermark(&self) -> Self::Watermark {
        self.version
    }
}

fn assert_watermark<T: MonotonicWatermark>() {}

#[test]
fn std_integer_watermark_implements_marker_trait() {
    assert_watermark::<i8>();
    assert_watermark::<i16>();
    assert_watermark::<i32>();
    assert_watermark::<i64>();
    assert_watermark::<i128>();
    assert_watermark::<isize>();
    assert_watermark::<u8>();
    assert_watermark::<u16>();
    assert_watermark::<u32>();
    assert_watermark::<u64>();
    assert_watermark::<u128>();
    assert_watermark::<usize>();
}

#[test]
fn system_time_watermark_implements_marker_trait() {
    assert_watermark::<std::time::SystemTime>();
}

#[test]
fn tuple_watermark_implements_marker_trait() {
    assert_watermark::<(i64, u64)>();
    assert_watermark::<(i64, u64, i32)>();
    assert_watermark::<(i64, u64, i32, u128)>();
}

#[test]
fn derive_emits_delta_sync_cacheable_when_watermark_field_is_present() {
    let item = WatermarkedItem {
        id: 7,
        updated_at: 42,
        shard: 3,
    };

    assert_eq!(<WatermarkedItem as Cacheable>::id(&item), 7);
    assert_eq!(
        <WatermarkedItem as DeltaSyncCacheable>::watermark(&item),
        42
    );
    assert_watermark::<<WatermarkedItem as DeltaSyncCacheable>::Watermark>();
}

#[test]
fn hand_written_delta_sync_cacheable_impls_work() {
    let item = ManualWatermarkedItem {
        id: 11,
        version: (99, 3),
    };
    let fields = ManualWatermarkedItem::fields();

    assert_eq!(*fields.id.extract(&item), 11);
    assert_eq!(*fields.version.extract(&item), (99, 3));
    assert_eq!(item.watermark(), (99, 3));
    assert_watermark::<<ManualWatermarkedItem as DeltaSyncCacheable>::Watermark>();
}

#[cfg(feature = "watermark-time")]
#[test]
fn time_watermarks_are_feature_gated() {
    assert_watermark::<time::OffsetDateTime>();
    assert_watermark::<time::Date>();
    assert_watermark::<time::PrimitiveDateTime>();
}

#[cfg(feature = "watermark-chrono")]
#[test]
fn chrono_watermarks_are_feature_gated() {
    assert_watermark::<chrono::DateTime<chrono::Utc>>();
    assert_watermark::<chrono::DateTime<chrono::FixedOffset>>();
    assert_watermark::<chrono::NaiveDateTime>();
    assert_watermark::<chrono::NaiveDate>();
}

#[cfg(not(any(feature = "watermark-time", feature = "watermark-chrono")))]
#[test]
fn rejected_watermark_types_fail_to_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/watermark_*.rs");
}
