#![cfg(all(feature = "serde", feature = "runtime-tokio"))]

use sassi::{CacheBackend, Cacheable, Field, FileBackend};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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

const FILE_ENTRY_KIND: u8 = 0x02;

fn keyspace(namespace: Option<&str>) -> sassi::BackendKeyspace {
    sassi::BackendKeyspace {
        namespace: namespace.map(Arc::from),
        type_name: <E as Cacheable>::cache_type_name(),
    }
}

fn walkdir(path: PathBuf) -> Vec<PathBuf> {
    if path.is_dir() {
        std::fs::read_dir(path)
            .unwrap()
            .flat_map(|entry| walkdir(entry.unwrap().path()))
            .collect()
    } else {
        vec![path]
    }
}

fn find_files_with_extension(root: &std::path::Path, extension: &str) -> Vec<PathBuf> {
    walkdir(root.to_path_buf())
        .into_iter()
        .filter(|path| path.extension().is_some_and(|ext| ext == extension))
        .collect()
}

#[tokio::test]
async fn file_backend_round_trips_binary_file_entry() {
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
    assert_eq!(loaded, Some(value.clone()));

    let sassi_files = find_files_with_extension(dir.path(), "sassi");
    assert_eq!(
        sassi_files.len(),
        1,
        "exactly one .sassi data file should exist"
    );
    let data = std::fs::read(&sassi_files[0]).unwrap();

    assert_eq!(&data[..8], b"SASSI\0W\0", "binary header magic");
    assert_eq!(
        u16::from_le_bytes([data[8], data[9]]),
        sassi::wire::WIRE_FORMAT_MAJOR,
        "wire major"
    );
    assert_eq!(data[10], FILE_ENTRY_KIND, "kind byte must be file-entry");
    assert_eq!(data[11], 0, "flags byte must be zero");

    assert!(
        find_files_with_extension(dir.path(), "json").is_empty(),
        "beta.2 file backend must never emit .json data files"
    );
    assert!(
        find_files_with_extension(dir.path(), "ttl").is_empty(),
        "beta.2 file backend must never emit .ttl sidecar files"
    );
}

#[tokio::test]
async fn file_backend_ttl_expiry_removes_expired_entry_before_payload_decode() {
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

    // Corrupt the postcard payload after the expiry prefix to prove
    // the get path observes expiry before attempting to decode the body.
    let sassi_files = find_files_with_extension(dir.path(), "sassi");
    assert_eq!(sassi_files.len(), 1);
    let mut bytes = std::fs::read(&sassi_files[0]).unwrap();
    let payload_start = bytes
        .len()
        .checked_sub(1)
        .expect("file should have at least one payload byte");
    for byte in &mut bytes[payload_start..] {
        *byte = 0xff;
    }
    std::fs::write(&sassi_files[0], &bytes).unwrap();

    assert_eq!(
        backend.get(&keyspace(None), &9_i64).await.unwrap(),
        None::<E>,
        "expired entry should be removed without decoding the payload"
    );
    assert!(
        find_files_with_extension(dir.path(), "sassi").is_empty(),
        "expired entry's .sassi file should be removed on read"
    );
}

#[tokio::test]
async fn file_backend_uses_sassi_extension_and_ignores_json_cache_files() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());
    let value = E {
        id: 100,
        label: "fresh".into(),
    };

    backend
        .put(&keyspace(None), &value.id(), &value, None)
        .await
        .unwrap();
    let sassi_files = find_files_with_extension(dir.path(), "sassi");
    assert_eq!(sassi_files.len(), 1);

    // Drop a stray beta.1-style `.json` sibling alongside the new
    // `.sassi` file. The backend must continue to read the `.sassi`
    // record and must not fall back to the legacy file.
    let json_sibling = sassi_files[0].with_extension("json");
    std::fs::write(
        &json_sibling,
        br#"{"__sassi_v":0,"payload":{"id":100,"label":"old"}}"#,
    )
    .unwrap();

    let loaded = backend.get(&keyspace(None), &100).await.unwrap();
    assert_eq!(loaded, Some(value));
    assert!(
        json_sibling.exists(),
        "beta.2 backend should leave stray .json files untouched"
    );
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
async fn file_backend_invalidate_removes_sassi_file() {
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
    assert_eq!(
        find_files_with_extension(dir.path(), "sassi").len(),
        1,
        "only the surviving id's .sassi file should remain"
    );
}

#[tokio::test]
async fn file_backend_invalidate_all_removes_keyspace_directory() {
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
    assert_eq!(
        find_files_with_extension(dir.path(), "sassi").len(),
        1,
        "drop keyspace .sassi files should be removed by invalidate_all"
    );
}
