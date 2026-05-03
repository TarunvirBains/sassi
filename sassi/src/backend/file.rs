//! Filesystem-backed L2 backend for development, tests, and simple local
//! persistence.
//!
//! This backend implements the async [`CacheBackend`](crate::CacheBackend) trait
//! with blocking `std::fs` calls. It keeps the core crate dependency-light, but
//! it is not designed for production request paths where filesystem latency
//! should be moved off the async executor.

use crate::backend::{BackendKeyspace, CacheBackend, encode_hex, keyspace_storage_key};
use crate::cacheable::Cacheable;
use crate::error::BackendError;
use crate::wire;
use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// File-backed cache backend.
///
/// Values are stored as Sassi wire-envelope JSON files. TTL metadata is
/// embedded in the same envelope so value and expiry are published with one
/// atomic file rename. Older `.ttl` sidecars are read for compatibility but are
/// ignored once inline expiry metadata is present.
///
/// This backend uses blocking filesystem operations inside async trait methods.
/// Use it for development, tests, and simple local persistence. For request-path
/// production traffic, prefer a backend that performs non-blocking I/O or moves
/// filesystem work to a blocking thread pool.
#[derive(Debug, Clone)]
pub struct FileBackend {
    root: PathBuf,
}

#[derive(Serialize)]
struct FileEnvelopeRef<'a, T: ?Sized> {
    #[serde(rename = "__sassi_v")]
    version: u64,
    #[serde(rename = "__sassi_has_inline_expiry")]
    has_inline_expiry: bool,
    #[serde(rename = "__sassi_expires_at_ms")]
    expires_at_ms: Option<u128>,
    payload: &'a T,
}

#[derive(Deserialize)]
struct FileEnvelopeMetadata {
    #[serde(rename = "__sassi_has_inline_expiry", default)]
    has_inline_expiry: bool,
    #[serde(rename = "__sassi_expires_at_ms", default)]
    expires_at_ms: Option<u128>,
}

impl FileBackend {
    /// Create a backend rooted at `root`.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn data_path<T: Cacheable>(
        &self,
        keyspace: &BackendKeyspace,
        id: &T::Id,
    ) -> Result<PathBuf, BackendError>
    where
        T::Id: Serialize,
    {
        Ok(self
            .root
            .join(keyspace_storage_key::<T>(keyspace, id)?)
            .with_extension("json"))
    }

    fn keyspace_dir(&self, keyspace: &BackendKeyspace) -> PathBuf {
        let namespace = match &keyspace.namespace {
            Some(ns) => format!("ns_{}", encode_hex(ns.as_bytes())),
            None => "ns_none".to_owned(),
        };
        let type_part = format!("ty_{}", encode_hex(keyspace.type_name.as_bytes()));
        self.root.join(namespace).join(type_part)
    }
}

#[async_trait]
impl<T> CacheBackend<T> for FileBackend
where
    T: Cacheable + Serialize + DeserializeOwned,
    T::Id: Serialize + DeserializeOwned,
{
    async fn get(&self, keyspace: &BackendKeyspace, id: &T::Id) -> Result<Option<T>, BackendError> {
        let path = self.data_path::<T>(keyspace, id)?;
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(BackendError::Network(err.to_string())),
        };
        if is_expired(&path, &bytes)? {
            remove_pair(&path)?;
            return Ok(None);
        }
        Ok(Some(wire::from_slice(&bytes)?))
    }

    async fn put(
        &self,
        keyspace: &BackendKeyspace,
        id: &T::Id,
        value: &T,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let path = self.data_path::<T>(keyspace, id)?;
        let parent = path
            .parent()
            .ok_or_else(|| BackendError::Other("backend path has no parent".into()))?;
        std::fs::create_dir_all(parent).map_err(|err| BackendError::Network(err.to_string()))?;

        let expires_at_ms = ttl
            .and_then(|ttl| SystemTime::now().checked_add(ttl))
            .map(|deadline| {
                deadline
                    .duration_since(UNIX_EPOCH)
                    .map_err(|err| BackendError::Other(err.into()))
                    .map(|duration| duration.as_millis())
            })
            .transpose()?;
        let bytes = file_wire_to_vec(value, expires_at_ms)?;
        write_atomic(&path, &bytes)?;

        remove_legacy_ttl_sidecar_best_effort(&path);

        Ok(())
    }

    async fn invalidate(&self, keyspace: &BackendKeyspace, id: &T::Id) -> Result<(), BackendError> {
        let path = self.data_path::<T>(keyspace, id)?;
        remove_pair(&path)
    }

    async fn invalidate_all(&self, keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        let dir = self.keyspace_dir(keyspace);
        match std::fs::remove_dir_all(dir) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BackendError::Network(err.to_string())),
        }
    }
}

fn ttl_path(path: &Path) -> PathBuf {
    path.with_extension("ttl")
}

fn file_wire_to_vec<T: Serialize + ?Sized>(
    payload: &T,
    expires_at_ms: Option<u128>,
) -> Result<Vec<u8>, BackendError> {
    let envelope = FileEnvelopeRef {
        version: wire::WIRE_FORMAT_MAJOR,
        has_inline_expiry: true,
        expires_at_ms,
        payload,
    };
    serde_json::to_vec(&envelope).map_err(BackendError::from)
}

fn is_expired(path: &Path, bytes: &[u8]) -> Result<bool, BackendError> {
    let metadata: FileEnvelopeMetadata = serde_json::from_slice(bytes)?;
    if metadata.has_inline_expiry {
        let now_ms = now_millis()?;
        let expired = metadata
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= now_ms);
        if !expired {
            remove_legacy_ttl_sidecar_best_effort(path);
        }
        return Ok(expired);
    }

    let meta_path = ttl_path(path);
    let raw = match std::fs::read_to_string(&meta_path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(BackendError::Network(err.to_string())),
    };
    let expires_at_ms = raw
        .trim()
        .parse::<u128>()
        .map_err(|err| BackendError::Serialization(err.to_string()))?;
    Ok(expires_at_ms <= now_millis()?)
}

fn now_millis() -> Result<u128, BackendError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| BackendError::Other(err.into()))?
        .as_millis())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), BackendError> {
    let parent = path
        .parent()
        .ok_or_else(|| BackendError::Other("backend path has no parent".into()))?;
    std::fs::create_dir_all(parent).map_err(|err| BackendError::Network(err.to_string()))?;
    let temp = temp_path(parent);
    std::fs::write(&temp, bytes).map_err(|err| BackendError::Network(err.to_string()))?;
    std::fs::rename(&temp, path).map_err(|err| {
        let _ = std::fs::remove_file(&temp);
        BackendError::Network(err.to_string())
    })
}

fn temp_path(parent: &Path) -> PathBuf {
    parent.join(format!(
        ".tmp-{}-{}-{:016x}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed),
        fastrand::u64(..)
    ))
}

fn remove_pair(path: &Path) -> Result<(), BackendError> {
    remove_file_idempotent(path)?;
    remove_file_idempotent(&ttl_path(path))
}

fn remove_legacy_ttl_sidecar_best_effort(path: &Path) {
    let _ = std::fs::remove_file(ttl_path(path));
}

fn remove_file_idempotent(path: &Path) -> Result<(), BackendError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(BackendError::Network(err.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_path_includes_entropy_beyond_pid_and_counter() {
        let dir = Path::new("/tmp/sassi-test");
        let first = temp_path(dir);
        let second = temp_path(dir);

        let first_name = first.file_name().unwrap().to_string_lossy();
        let second_name = second.file_name().unwrap().to_string_lossy();

        assert_ne!(first_name, second_name);
        assert!(
            first_name.rsplit('-').next().unwrap().len() >= 16,
            "temp file suffix should include random entropy for shared-volume process collisions"
        );
    }
}
