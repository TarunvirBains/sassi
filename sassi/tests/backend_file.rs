#![cfg(all(feature = "serde", feature = "runtime-tokio"))]

use sassi::{CacheBackend, Cacheable, Field, FileBackend};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

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

fn keyspace(namespace: Option<&str>) -> sassi::BackendKeyspace {
    sassi::BackendKeyspace {
        namespace: namespace.map(Arc::from),
        type_name: std::any::type_name::<E>(),
    }
}

#[tokio::test]
async fn file_backend_round_trips_wire_envelope_payload() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());
    let value = E {
        id: 7,
        label: "seven".into(),
    };

    backend
        .put(&keyspace(Some("env/a")), &value.id(), &value, None)
        .await
        .unwrap();

    let loaded = backend.get(&keyspace(Some("env/a")), &7).await.unwrap();
    assert_eq!(loaded, Some(value));

    let files = std::fs::read_dir(dir.path())
        .unwrap()
        .flat_map(|entry| walkdir(entry.unwrap().path()))
        .collect::<Vec<_>>();
    let data = files
        .iter()
        .find(|path| path.extension().is_some_and(|ext| ext == "json"))
        .map(std::fs::read)
        .unwrap()
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(envelope["__sassi_v"], 0);
    assert_eq!(envelope["payload"]["label"], "seven");
}

#[tokio::test]
async fn file_backend_ttl_expiry_removes_expired_entry() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());
    let value = E {
        id: 9,
        label: "nine".into(),
    };

    backend
        .put(&keyspace(None), &value.id(), &value, Some(Duration::ZERO))
        .await
        .unwrap();

    assert_eq!(
        backend.get(&keyspace(None), &9_i64).await.unwrap(),
        None::<E>
    );
}

#[tokio::test]
async fn file_backend_ignores_stale_legacy_ttl_sidecar_for_fresh_inline_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());
    let value = E {
        id: 10,
        label: "fresh".into(),
    };

    backend
        .put(&keyspace(None), &value.id(), &value, None)
        .await
        .unwrap();

    let data_path = std::fs::read_dir(dir.path())
        .unwrap()
        .flat_map(|entry| walkdir(entry.unwrap().path()))
        .find(|path| path.extension().is_some_and(|ext| ext == "json"))
        .expect("data file should exist");
    std::fs::write(data_path.with_extension("ttl"), b"0").unwrap();

    let loaded = backend.get(&keyspace(None), &10).await.unwrap();

    assert_eq!(loaded, Some(value));
    assert!(
        !data_path.with_extension("ttl").exists(),
        "stale legacy ttl sidecar should be removed after the fresh value is read"
    );
}

#[tokio::test]
async fn file_backend_reads_fresh_inline_value_when_stale_sidecar_cleanup_fails() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());
    let value = E {
        id: 11,
        label: "fresh".into(),
    };

    backend
        .put(&keyspace(None), &value.id(), &value, None)
        .await
        .unwrap();

    let data_path = std::fs::read_dir(dir.path())
        .unwrap()
        .flat_map(|entry| walkdir(entry.unwrap().path()))
        .find(|path| path.extension().is_some_and(|ext| ext == "json"))
        .expect("data file should exist");
    std::fs::create_dir(data_path.with_extension("ttl")).unwrap();

    let loaded = backend.get(&keyspace(None), &11).await.unwrap();

    assert_eq!(loaded, Some(value));
}

#[tokio::test]
async fn file_backend_namespaces_do_not_collide() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());

    backend
        .put(
            &keyspace(Some("env-a")),
            &1_i64,
            &E {
                id: 1,
                label: "a".into(),
            },
            None,
        )
        .await
        .unwrap();
    backend
        .put(
            &keyspace(Some("env-b")),
            &1_i64,
            &E {
                id: 1,
                label: "b".into(),
            },
            None,
        )
        .await
        .unwrap();

    let loaded_a: E = backend
        .get(&keyspace(Some("env-a")), &1_i64)
        .await
        .unwrap()
        .unwrap();
    let loaded_b: E = backend
        .get(&keyspace(Some("env-b")), &1_i64)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(loaded_a.label, "a");
    assert_eq!(loaded_b.label, "b");
}

#[tokio::test]
async fn file_backend_invalidate_removes_only_requested_id() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());
    let keyspace = keyspace(None);

    backend
        .put(
            &keyspace,
            &1,
            &E {
                id: 1,
                label: "one".into(),
            },
            None,
        )
        .await
        .unwrap();
    backend
        .put(
            &keyspace,
            &2,
            &E {
                id: 2,
                label: "two".into(),
            },
            None,
        )
        .await
        .unwrap();

    <FileBackend as CacheBackend<E>>::invalidate(&backend, &keyspace, &1_i64)
        .await
        .unwrap();

    let remaining: E = backend.get(&keyspace, &2_i64).await.unwrap().unwrap();
    assert_eq!(backend.get(&keyspace, &1_i64).await.unwrap(), None::<E>);
    assert_eq!(remaining.label, "two");
}

#[tokio::test]
async fn file_backend_invalidate_all_is_namespace_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());
    let keep = keyspace(Some("keep"));
    let drop = keyspace(Some("drop"));

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

    <FileBackend as CacheBackend<E>>::invalidate_all(&backend, &drop)
        .await
        .unwrap();

    let remaining: E = backend.get(&keep, &1_i64).await.unwrap().unwrap();
    assert_eq!(backend.get(&drop, &1_i64).await.unwrap(), None::<E>);
    assert_eq!(remaining.label, "keep");
}

fn walkdir(path: std::path::PathBuf) -> Vec<std::path::PathBuf> {
    if path.is_dir() {
        std::fs::read_dir(path)
            .unwrap()
            .flat_map(|entry| walkdir(entry.unwrap().path()))
            .collect()
    } else {
        vec![path]
    }
}
