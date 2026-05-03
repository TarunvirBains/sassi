use futures::StreamExt;
use sassi::{BackendInvalidation, BackendKeyspace, CacheBackend, Cacheable, Field};
use sassi_cache_redis::RedisBackend;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct E {
    id: i64,
    label: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct F {
    id: i64,
    label: String,
}

#[derive(Default)]
struct FFields {
    #[allow(dead_code)]
    id: Field<F, i64>,
}

impl Cacheable for F {
    type Id = i64;
    type Fields = FFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> FFields {
        FFields {
            id: Field::new("id", |f| &f.id),
        }
    }
}

fn redis_client() -> Option<redis::Client> {
    std::env::var("REDIS_URL")
        .ok()
        .and_then(|url| redis::Client::open(url).ok())
}

fn namespace(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("sassi-test-{label}-{nanos}")
}

fn keyspace<T: Cacheable>(namespace: String) -> BackendKeyspace {
    BackendKeyspace {
        namespace: Some(Arc::from(namespace)),
        type_name: std::any::type_name::<T>(),
    }
}

#[tokio::test]
async fn redis_invalidation_stream_receives_id_and_all_messages() {
    let Some(client) = redis_client() else {
        eprintln!("skipping redis test because REDIS_URL is not set");
        return;
    };
    let writer = RedisBackend::<E>::new(client.clone());
    let subscriber = RedisBackend::<E>::new(client);
    let keyspace = keyspace::<E>(namespace("stream"));
    let mut stream = subscriber.invalidation_stream(keyspace.clone());

    writer
        .put(
            &keyspace,
            &7,
            &E {
                id: 7,
                label: "seven".into(),
            },
            None,
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    writer.invalidate(&keyspace, &7).await.unwrap();

    let message = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message, BackendInvalidation::Id(7));

    writer.invalidate_all(&keyspace).await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message, BackendInvalidation::All);
}

#[tokio::test]
async fn redis_invalidate_all_is_namespace_scoped() {
    let Some(client) = redis_client() else {
        eprintln!("skipping redis test because REDIS_URL is not set");
        return;
    };
    let backend = RedisBackend::<E>::new(client);
    let keep = keyspace::<E>(namespace("keep"));
    let drop = keyspace::<E>(namespace("drop"));

    backend
        .put(
            &keep,
            &1,
            &E {
                id: 1,
                label: "keep".into(),
            },
            None,
        )
        .await
        .unwrap();
    backend
        .put(
            &drop,
            &1,
            &E {
                id: 1,
                label: "drop".into(),
            },
            None,
        )
        .await
        .unwrap();

    backend.invalidate_all(&drop).await.unwrap();

    assert_eq!(backend.get(&drop, &1).await.unwrap(), None);
    assert_eq!(backend.get(&keep, &1).await.unwrap().unwrap().label, "keep");
}

#[tokio::test]
async fn redis_invalidate_all_is_type_scoped() {
    let Some(client) = redis_client() else {
        eprintln!("skipping redis test because REDIS_URL is not set");
        return;
    };
    let e_backend = RedisBackend::<E>::new(client.clone());
    let f_backend = RedisBackend::<F>::new(client);
    let ns = namespace("type-scope");
    let e_keyspace = keyspace::<E>(ns.clone());
    let f_keyspace = keyspace::<F>(ns);

    e_backend
        .put(
            &e_keyspace,
            &1,
            &E {
                id: 1,
                label: "drop".into(),
            },
            None,
        )
        .await
        .unwrap();
    f_backend
        .put(
            &f_keyspace,
            &1,
            &F {
                id: 1,
                label: "keep".into(),
            },
            None,
        )
        .await
        .unwrap();

    e_backend.invalidate_all(&e_keyspace).await.unwrap();

    assert_eq!(e_backend.get(&e_keyspace, &1).await.unwrap(), None);
    assert_eq!(
        f_backend.get(&f_keyspace, &1).await.unwrap().unwrap().label,
        "keep"
    );
}
