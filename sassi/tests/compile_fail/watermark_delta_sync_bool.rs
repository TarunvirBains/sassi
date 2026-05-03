//! `DeltaSyncCacheable::Watermark` must implement `MonotonicWatermark`.

use sassi::{Cacheable, DeltaSyncCacheable, Field};

#[derive(Clone)]
struct Item {
    id: i64,
    deleted: bool,
}

#[derive(Default)]
struct ItemFields {
    id: Field<Item, i64>,
    deleted: Field<Item, bool>,
}

impl Cacheable for Item {
    type Id = i64;
    type Fields = ItemFields;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {
        ItemFields {
            id: Field::new("id", |item| &item.id),
            deleted: Field::new("deleted", |item| &item.deleted),
        }
    }
}

impl DeltaSyncCacheable for Item {
    type Watermark = bool;

    fn watermark(&self) -> Self::Watermark {
        self.deleted
    }
}

fn main() {}
