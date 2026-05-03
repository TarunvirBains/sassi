//! In-memory L2 backend used for round-trip tests.

use crate::backend::{
    BackendKeyspace, CacheBackend, keyspace_storage_key, keyspace_storage_key_prefix,
};
use crate::cacheable::Cacheable;
use crate::error::BackendError;
use crate::wire;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Serialize, de::DeserializeOwned};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct MemoryCell {
    bytes: Vec<u8>,
    expires_at: Option<Instant>,
}

/// Separate in-memory backend that stores wire-envelope bytes.
///
/// This backend is not a replacement for L1; it exists to test the
/// `CacheBackend` path without a Redis or filesystem dependency.
#[derive(Default)]
pub struct MemoryBackend {
    entries: DashMap<String, MemoryCell>,
}

impl MemoryBackend {
    fn key<T: Cacheable>(
        &self,
        keyspace: &BackendKeyspace,
        id: &T::Id,
    ) -> Result<String, BackendError>
    where
        T::Id: Serialize,
    {
        keyspace_storage_key::<T>(keyspace, id)
    }
}

#[async_trait]
impl<T> CacheBackend<T> for MemoryBackend
where
    T: Cacheable + Serialize + DeserializeOwned,
    T::Id: Serialize + DeserializeOwned,
{
    async fn get(&self, keyspace: &BackendKeyspace, id: &T::Id) -> Result<Option<T>, BackendError> {
        let key = self.key::<T>(keyspace, id)?;
        let Some(cell) = self.entries.get(&key) else {
            return Ok(None);
        };
        if cell
            .expires_at
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            drop(cell);
            self.entries.remove(&key);
            return Ok(None);
        }
        Ok(Some(wire::from_slice(&cell.bytes)?))
    }

    async fn put(
        &self,
        keyspace: &BackendKeyspace,
        id: &T::Id,
        value: &T,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let key = self.key::<T>(keyspace, id)?;
        let expires_at = ttl.and_then(|ttl| Instant::now().checked_add(ttl));
        let bytes = wire::to_vec(value)?;
        self.entries.insert(key, MemoryCell { bytes, expires_at });
        Ok(())
    }

    async fn invalidate(&self, keyspace: &BackendKeyspace, id: &T::Id) -> Result<(), BackendError> {
        let key = self.key::<T>(keyspace, id)?;
        self.entries.remove(&key);
        Ok(())
    }

    async fn invalidate_all(&self, keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        let prefix = keyspace_storage_key_prefix(keyspace);
        self.entries.retain(|key, _| !key.starts_with(&prefix));
        Ok(())
    }
}
