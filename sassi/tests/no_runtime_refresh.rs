#![cfg(not(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
)))]

use sassi::{
    Cacheable, DeltaPunnuFetcher, DeltaQuery, DeltaResult, DeltaSyncCacheable, FetchError, Field,
    Punnu, PunnuFetcher, RefreshMode,
};
use std::collections::HashSet;
use std::time::Duration;

#[derive(Debug, Clone)]
struct Item {
    id: u64,
    watermark: u64,
}

#[derive(Default)]
struct ItemFields {
    #[allow(dead_code)]
    id: Field<Item, u64>,
    #[allow(dead_code)]
    watermark: Field<Item, u64>,
}

impl Cacheable for Item {
    type Id = u64;
    type Fields = ItemFields;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {
        ItemFields {
            id: Field::new("id", |item| &item.id),
            watermark: Field::new("watermark", |item| &item.watermark),
        }
    }
}

impl DeltaSyncCacheable for Item {
    type Watermark = u64;

    fn watermark(&self) -> Self::Watermark {
        self.watermark
    }
}

#[derive(Clone)]
struct PeriodicFetcher;

#[derive(Clone)]
struct DeltaFetcher;

#[async_trait::async_trait]
impl PunnuFetcher<Item> for PeriodicFetcher {
    async fn fetch(&self) -> Result<Vec<Item>, FetchError> {
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl DeltaPunnuFetcher<Item> for DeltaFetcher {
    async fn fetch_delta(
        &self,
        _query: DeltaQuery<Item>,
    ) -> Result<DeltaResult<Item, u64>, FetchError> {
        Ok(DeltaResult::new(Vec::new(), HashSet::new()))
    }
}

#[test]
#[should_panic(
    expected = "Punnu::start_periodic_refresh requires `runtime-tokio` on native targets or `runtime-wasm` on wasm32"
)]
fn start_periodic_refresh_rejects_missing_target_runtime_feature() {
    let punnu = Punnu::<Item>::builder().build();
    let _ = punnu.start_periodic_refresh(
        Duration::from_secs(1),
        PeriodicFetcher,
        RefreshMode::UpsertOnly,
    );
}

#[test]
#[should_panic(
    expected = "Punnu::start_delta_refresh requires `runtime-tokio` on native targets or `runtime-wasm` on wasm32"
)]
fn start_delta_refresh_rejects_missing_target_runtime_feature() {
    let punnu = Punnu::<Item>::builder().build();
    let _ = punnu.start_delta_refresh(Duration::from_secs(1), DeltaFetcher);
}
