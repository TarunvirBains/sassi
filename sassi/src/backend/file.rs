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
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

const EXPIRY_NONE: u8 = 0;
const EXPIRY_UNIX_MS: u8 = 1;
const FILE_EXPIRY_PREFIX_LEN: usize = 9;

/// File-backed cache backend.
///
/// Values are stored as Sassi binary wire-container records with a
/// `.sassi` extension. The on-disk body carries an inline expiry tag
/// followed by a postcard-encoded payload, so value and expiry are
/// published with one atomic file rename. Beta.2 does not read the
/// beta.1 `.json` cache files or `.ttl` sidecars; operators should
/// treat any leftover beta.1 files as cold misses and clear them
/// during upgrade.
///
/// This backend uses blocking filesystem operations inside async trait methods.
/// Use it for development, tests, and simple local persistence. For request-path
/// production traffic, prefer a backend that performs non-blocking I/O or moves
/// filesystem work to a blocking thread pool.
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
            .with_extension("sassi"))
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
        let (_, payload) = file_wire_from_slice::<T>(&bytes)?;
        match payload {
            Some(value) => Ok(Some(value)),
            None => {
                remove_file_idempotent(&path)?;
                Ok(None)
            }
        }
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

        let expires_at_ms = match ttl {
            Some(ttl) => Some(absolute_expiry_millis(ttl)?),
            None => None,
        };
        let bytes = file_wire_to_vec(value, expires_at_ms)?;
        write_atomic(&path, &bytes)?;

        Ok(())
    }

    async fn invalidate(&self, keyspace: &BackendKeyspace, id: &T::Id) -> Result<(), BackendError> {
        let path = self.data_path::<T>(keyspace, id)?;
        remove_file_idempotent(&path)
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

fn file_wire_to_vec<T: Cacheable + Serialize>(
    payload: &T,
    expires_at_ms: Option<u64>,
) -> Result<Vec<u8>, BackendError> {
    let mut out = Vec::new();
    wire::encode_header::<T>(wire::WireKind::FileEntry, &mut out)?;
    match expires_at_ms {
        Some(ms) => {
            out.push(EXPIRY_UNIX_MS);
            out.extend_from_slice(&ms.to_le_bytes());
        }
        None => {
            out.push(EXPIRY_NONE);
            out.extend_from_slice(&0_u64.to_le_bytes());
        }
    }
    let body = postcard::to_allocvec(payload)
        .map_err(|err| BackendError::Serialization(err.to_string()))?;
    out.extend_from_slice(&body);
    Ok(out)
}

fn file_wire_from_slice<T: Cacheable + DeserializeOwned>(
    bytes: &[u8],
) -> Result<(Option<u64>, Option<T>), BackendError> {
    let body = wire::decode_header::<T>(bytes, wire::WireKind::FileEntry)?;
    if body.len() < FILE_EXPIRY_PREFIX_LEN {
        return Err(BackendError::Serialization(
            "file entry body too short".into(),
        ));
    }
    let tag = body[0];
    let expires_at_ms = u64::from_le_bytes(body[1..9].try_into().expect("slice length checked"));
    let expiry = match tag {
        EXPIRY_NONE => None,
        EXPIRY_UNIX_MS => Some(expires_at_ms),
        other => {
            return Err(BackendError::Serialization(format!(
                "unsupported file expiry tag {other}"
            )));
        }
    };
    let now_ms = now_millis()?;
    if expiry.is_some_and(|ms| ms <= now_ms) {
        return Ok((expiry, None));
    }
    let payload =
        wire::decode_postcard_exact(&body[FILE_EXPIRY_PREFIX_LEN..]).map_err(BackendError::from)?;
    Ok((expiry, Some(payload)))
}

fn absolute_expiry_millis(ttl: Duration) -> Result<u64, BackendError> {
    let deadline = SystemTime::now().checked_add(ttl).ok_or_else(|| {
        BackendError::Serialization("file expiry exceeds u64 milliseconds".into())
    })?;
    let duration = deadline
        .duration_since(UNIX_EPOCH)
        .map_err(|err| BackendError::Other(err.into()))?;
    duration
        .as_millis()
        .try_into()
        .map_err(|_| BackendError::Serialization("file expiry exceeds u64 milliseconds".into()))
}

fn now_millis() -> Result<u64, BackendError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| BackendError::Other(err.into()))?;
    duration
        .as_millis()
        .try_into()
        .map_err(|_| BackendError::Serialization("file expiry exceeds u64 milliseconds".into()))
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
