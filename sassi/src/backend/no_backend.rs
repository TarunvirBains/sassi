//! Explicit no-L2 backend marker.

use crate::backend::{BackendKeyspace, CacheBackend};
use crate::cacheable::Cacheable;
use crate::error::BackendError;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::time::Duration;

/// Explicit marker backend for a Punnu with no L2 cache.
///
/// Reads always miss, writes and invalidations succeed, and the
/// invalidation stream is empty.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoBackend;

#[async_trait]
impl<T> CacheBackend<T> for NoBackend
where
    T: Cacheable + Serialize + DeserializeOwned,
    T::Id: Serialize + DeserializeOwned,
{
    async fn get(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &T::Id,
    ) -> Result<Option<T>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &T::Id,
        _value: &T,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &T::Id,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}
