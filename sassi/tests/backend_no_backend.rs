#![cfg(all(feature = "serde", feature = "runtime-tokio"))]

use futures::{FutureExt, StreamExt};
use sassi::{BackendKeyspace, CacheBackend, Cacheable, Field, NoBackend};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct E {
    id: i64,
}

#[derive(Default)]
struct EFields {
    #[allow(dead_code)]
    id: Field<E, i64>,
}

impl Cacheable for E {
    type Id = i64;
    type Fields = EFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> EFields {
        EFields {
            id: Field::new("id", |e| &e.id),
        }
    }
}

fn keyspace() -> BackendKeyspace {
    BackendKeyspace {
        namespace: Some(Arc::from("test-env")),
        type_name: std::any::type_name::<E>(),
    }
}

#[tokio::test]
async fn no_backend_get_should_always_miss() {
    let backend = NoBackend;

    let result = <NoBackend as CacheBackend<E>>::get(&backend, &keyspace(), &7)
        .await
        .unwrap();

    assert_eq!(result, None);
}

#[tokio::test]
async fn no_backend_writes_and_invalidations_should_succeed() {
    let backend = NoBackend;
    let value = E { id: 7 };

    <NoBackend as CacheBackend<E>>::put(
        &backend,
        &keyspace(),
        &value.id(),
        &value,
        Some(std::time::Duration::from_secs(5)),
    )
    .await
    .unwrap();
    <NoBackend as CacheBackend<E>>::invalidate(&backend, &keyspace(), &value.id())
        .await
        .unwrap();
    <NoBackend as CacheBackend<E>>::invalidate_all(&backend, &keyspace())
        .await
        .unwrap();
}

#[test]
fn no_backend_invalidation_stream_should_be_empty() {
    let backend = NoBackend;
    let mut stream = <NoBackend as CacheBackend<E>>::invalidation_stream(&backend, keyspace());

    let next = stream.next().now_or_never();

    assert!(matches!(next, Some(None)));
}
