//! Filesystem-backed L2 backend for development and tests.

use crate::backend::{BackendKeyspace, CacheBackend, encode_hex, keyspace_storage_key};
use crate::cacheable::Cacheable;
use crate::error::BackendError;
use crate::wire;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// File-backed cache backend.
///
/// Values are stored as Sassi wire-envelope JSON files. TTL metadata is
/// stored in a sidecar file next to the envelope so the payload file
/// remains a plain wire envelope.
#[derive(Debug, Clone)]
pub struct FileBackend {
    root: PathBuf,
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
        if is_expired(&path)? {
            remove_pair(&path)?;
            return Ok(None);
        }
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(BackendError::Network(err.to_string())),
        };
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

        let bytes = wire::to_vec(value)?;
        write_atomic(&path, &bytes)?;

        let meta_path = ttl_path(&path);
        match ttl.and_then(|ttl| SystemTime::now().checked_add(ttl)) {
            Some(deadline) => {
                let millis = deadline
                    .duration_since(UNIX_EPOCH)
                    .map_err(|err| BackendError::Other(err.into()))?
                    .as_millis()
                    .to_string();
                write_atomic(&meta_path, millis.as_bytes())?;
            }
            None => {
                remove_file_idempotent(&meta_path)?;
            }
        }

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

fn is_expired(path: &Path) -> Result<bool, BackendError> {
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
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| BackendError::Other(err.into()))?
        .as_millis();
    Ok(expires_at_ms <= now_ms)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), BackendError> {
    let parent = path
        .parent()
        .ok_or_else(|| BackendError::Other("backend path has no parent".into()))?;
    std::fs::create_dir_all(parent).map_err(|err| BackendError::Network(err.to_string()))?;
    let temp = parent.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&temp, bytes).map_err(|err| BackendError::Network(err.to_string()))?;
    std::fs::rename(&temp, path).map_err(|err| {
        let _ = std::fs::remove_file(&temp);
        BackendError::Network(err.to_string())
    })
}

fn remove_pair(path: &Path) -> Result<(), BackendError> {
    remove_file_idempotent(path)?;
    remove_file_idempotent(&ttl_path(path))
}

fn remove_file_idempotent(path: &Path) -> Result<(), BackendError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(BackendError::Network(err.to_string())),
    }
}
