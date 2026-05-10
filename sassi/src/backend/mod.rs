//! Pluggable L2 cache backend interfaces and built-in implementations.
//!
//! A backend is scoped by [`BackendKeyspace`], which Sassi constructs
//! from [`crate::punnu::PunnuConfig::namespace`] and
//! [`crate::Cacheable::cache_type_name`]. Backend implementations must treat
//! that keyspace as the only namespace/type source of truth.

mod file;
mod memory;

use crate::Cacheable;
use crate::error::BackendError;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;
use std::time::Duration;

pub use file::FileBackend;
pub use memory::MemoryBackend;

/// Stream type used by [`CacheBackend::invalidation_stream`].
pub type BackendInvalidationStream<Id> =
    BoxStream<'static, Result<BackendInvalidation<Id>, BackendError>>;

/// Namespace/type scope for backend storage and invalidation channels.
///
/// `namespace` comes from [`crate::punnu::PunnuConfig::namespace`].
/// `type_name` is [`crate::Cacheable::cache_type_name`] for the cached type.
/// Backends should encode both components before putting them in
/// filesystem paths, Redis keys, channels, or other backend-native
/// identifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKeyspace {
    /// Optional deployment/application namespace.
    pub namespace: Option<Arc<str>>,
    /// Cached type label from [`crate::Cacheable::cache_type_name`].
    pub type_name: &'static str,
}

impl BackendKeyspace {
    /// Build the canonical keyspace for `T`.
    pub(crate) fn for_type<T: Cacheable>(namespace: Option<&str>) -> Self {
        Self {
            namespace: namespace.map(Arc::from),
            type_name: T::cache_type_name(),
        }
    }
}

/// Backend-driven invalidation message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub enum BackendInvalidation<Id> {
    /// Invalidate one id in the scoped type keyspace.
    Id(Id),
    /// Invalidate every resident L1 entry for the scoped type keyspace.
    All,
}

/// L2 cache backend for a single [`Cacheable`] payload type.
///
/// Backends receive a [`BackendKeyspace`] on every operation. They
/// should not carry an independent namespace because it could diverge
/// from the owning [`crate::punnu::Punnu`].
#[async_trait]
pub trait CacheBackend<T>: Send + Sync
where
    T: Cacheable + Serialize + DeserializeOwned,
    T::Id: Serialize + DeserializeOwned,
{
    /// Read an entry from the backend.
    async fn get(&self, keyspace: &BackendKeyspace, id: &T::Id) -> Result<Option<T>, BackendError>;

    /// Store an entry in the backend.
    async fn put(
        &self,
        keyspace: &BackendKeyspace,
        id: &T::Id,
        value: &T,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError>;

    /// Invalidate one backend entry and publish an id-scoped invalidation if supported.
    async fn invalidate(&self, keyspace: &BackendKeyspace, id: &T::Id) -> Result<(), BackendError>;

    /// Invalidate backend entries in this keyspace and publish an all-scoped
    /// invalidation if supported.
    ///
    /// Backend implementations may need to scan or batch-delete native storage.
    /// Unless an implementation documents stronger guarantees, this is not a
    /// quiescence barrier against concurrent writers in the same keyspace.
    async fn invalidate_all(&self, keyspace: &BackendKeyspace) -> Result<(), BackendError>;

    /// Subscribe to backend invalidations for one keyspace.
    fn invalidation_stream(&self, _keyspace: BackendKeyspace) -> BackendInvalidationStream<T::Id> {
        Box::pin(futures::stream::empty())
    }
}

pub(crate) trait BackendRuntime<T: Cacheable>: Send + Sync {
    fn get<'a>(
        &'a self,
        keyspace: &'a BackendKeyspace,
        id: &'a T::Id,
    ) -> BoxFuture<'a, Result<Option<T>, BackendError>>;

    fn put<'a>(
        &'a self,
        keyspace: &'a BackendKeyspace,
        id: &'a T::Id,
        value: &'a T,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), BackendError>>;

    fn invalidate<'a>(
        &'a self,
        keyspace: &'a BackendKeyspace,
        id: &'a T::Id,
    ) -> BoxFuture<'a, Result<(), BackendError>>;

    fn invalidation_stream(&self, keyspace: BackendKeyspace) -> BackendInvalidationStream<T::Id>;
}

struct BackendRuntimeAdapter<B> {
    backend: B,
}

pub(crate) fn erase_backend<T, B>(backend: B) -> Arc<dyn BackendRuntime<T>>
where
    T: Cacheable + Serialize + DeserializeOwned,
    T::Id: Serialize + DeserializeOwned,
    B: CacheBackend<T> + 'static,
{
    Arc::new(BackendRuntimeAdapter { backend })
}

impl<T, B> BackendRuntime<T> for BackendRuntimeAdapter<B>
where
    T: Cacheable + Serialize + DeserializeOwned,
    T::Id: Serialize + DeserializeOwned,
    B: CacheBackend<T>,
{
    fn get<'a>(
        &'a self,
        keyspace: &'a BackendKeyspace,
        id: &'a T::Id,
    ) -> BoxFuture<'a, Result<Option<T>, BackendError>> {
        Box::pin(self.backend.get(keyspace, id))
    }

    fn put<'a>(
        &'a self,
        keyspace: &'a BackendKeyspace,
        id: &'a T::Id,
        value: &'a T,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), BackendError>> {
        Box::pin(self.backend.put(keyspace, id, value, ttl))
    }

    fn invalidate<'a>(
        &'a self,
        keyspace: &'a BackendKeyspace,
        id: &'a T::Id,
    ) -> BoxFuture<'a, Result<(), BackendError>> {
        Box::pin(self.backend.invalidate(keyspace, id))
    }

    fn invalidation_stream(&self, keyspace: BackendKeyspace) -> BackendInvalidationStream<T::Id> {
        self.backend.invalidation_stream(keyspace)
    }
}

/// Encode a backend storage key for `id` under `keyspace`.
///
/// The id segment is the hex-encoded postcard serialization of `id`.
/// Postcard is already in scope under the `serde` feature (it carries
/// Sassi's binary value wire), so reusing it here removes the
/// historical dependence on `serde_json` for backend keys. The change
/// is wire-format-incompatible with stored records produced before
/// this version; see the v0.1.0 release readiness doc for the upgrade
/// note.
pub(crate) fn keyspace_storage_key<T>(
    keyspace: &BackendKeyspace,
    id: &T::Id,
) -> Result<String, BackendError>
where
    T: Cacheable,
    T::Id: Serialize,
{
    let id_bytes =
        postcard::to_allocvec(id).map_err(|err| BackendError::Serialization(err.to_string()))?;
    let id_part = format!("id_{}", encode_hex(&id_bytes));
    Ok(format!(
        "{}{}",
        keyspace_storage_key_prefix(keyspace),
        id_part
    ))
}

pub(crate) fn keyspace_storage_key_prefix(keyspace: &BackendKeyspace) -> String {
    let namespace = match &keyspace.namespace {
        Some(ns) => format!("ns_{}", encode_hex(ns.as_bytes())),
        None => "ns_none".to_owned(),
    };
    let type_part = format!("ty_{}", encode_hex(keyspace.type_name.as_bytes()));
    format!("{namespace}/{type_part}/")
}

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}
