//! Redis [`sassi::CacheBackend`] companion crate.

use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use redis::AsyncCommands;
use sassi::{BackendError, BackendInvalidation, BackendKeyspace, CacheBackend, Cacheable};
use serde::{Serialize, de::DeserializeOwned};
use std::cmp;
use std::marker::PhantomData;
use std::time::Duration;

const INITIAL_INVALIDATION_RECONNECT_DELAY: Duration = Duration::from_millis(10);
const MAX_INVALIDATION_RECONNECT_DELAY: Duration = Duration::from_millis(100);

/// Redis-backed Sassi L2 backend.
///
/// Keys and pub/sub channels are derived only from Sassi's
/// [`BackendKeyspace`]. The backend carries no independent namespace,
/// so `PunnuConfig::namespace` remains the single source of truth.
#[derive(Clone)]
pub struct RedisBackend<T> {
    client: redis::Client,
    _marker: PhantomData<fn() -> T>,
}

impl<T> RedisBackend<T> {
    /// Construct a backend from a Redis client.
    pub fn new(client: redis::Client) -> Self {
        Self {
            client,
            _marker: PhantomData,
        }
    }

    fn key(keyspace: &BackendKeyspace, id: &T::Id) -> Result<String, BackendError>
    where
        T: Cacheable,
        T::Id: Serialize,
    {
        let prefix = Self::key_prefix(keyspace);
        let id_json = serde_json::to_vec(id)?;
        Ok(format!("{prefix}:id_{}", encode_hex(&id_json)))
    }

    fn key_prefix(keyspace: &BackendKeyspace) -> String {
        let namespace = match &keyspace.namespace {
            Some(namespace) => format!("ns_{}", encode_hex(namespace.as_bytes())),
            None => "ns_none".to_owned(),
        };
        let type_part = format!("ty_{}", encode_hex(keyspace.type_name.as_bytes()));
        format!("sassi:{namespace}:{type_part}")
    }

    fn channel(keyspace: &BackendKeyspace) -> String {
        format!("{}:invalidate", Self::key_prefix(keyspace))
    }
}

#[async_trait]
impl<T> CacheBackend<T> for RedisBackend<T>
where
    T: Cacheable + Serialize + DeserializeOwned,
    T::Id: Serialize + DeserializeOwned,
{
    async fn get(&self, keyspace: &BackendKeyspace, id: &T::Id) -> Result<Option<T>, BackendError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(redis_error)?;
        let key = Self::key(keyspace, id)?;
        let raw: Option<Vec<u8>> = conn.get(key).await.map_err(redis_error)?;
        raw.map(|bytes| sassi::wire::from_slice(&bytes).map_err(BackendError::from))
            .transpose()
    }

    async fn put(
        &self,
        keyspace: &BackendKeyspace,
        id: &T::Id,
        value: &T,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(redis_error)?;
        let key = Self::key(keyspace, id)?;
        let bytes = sassi::wire::to_vec(value)?;
        match ttl {
            Some(ttl) if ttl.is_zero() => {
                conn.del::<_, ()>(&key).await.map_err(redis_error)?;
            }
            Some(ttl) => {
                let millis = ttl.as_millis().min(u64::MAX as u128) as u64;
                conn.pset_ex::<_, _, ()>(&key, bytes, millis)
                    .await
                    .map_err(redis_error)?;
            }
            None => {
                conn.set::<_, _, ()>(&key, bytes)
                    .await
                    .map_err(redis_error)?;
            }
        }
        Ok(())
    }

    async fn invalidate(&self, keyspace: &BackendKeyspace, id: &T::Id) -> Result<(), BackendError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(redis_error)?;
        let key = Self::key(keyspace, id)?;
        conn.del::<_, ()>(&key).await.map_err(redis_error)?;
        let payload = serde_json::to_vec(&BackendInvalidation::Id(id.clone()))?;
        conn.publish::<_, _, ()>(Self::channel(keyspace), payload)
            .await
            .map_err(redis_error)?;
        Ok(())
    }

    async fn invalidate_all(&self, keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(redis_error)?;
        let prefix = Self::key_prefix(keyspace);
        let mut cursor = 0_u64;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{prefix}:id_*"))
                .arg("COUNT")
                .arg(500_u64)
                .query_async(&mut conn)
                .await
                .map_err(redis_error)?;
            if !keys.is_empty() {
                redis::cmd("DEL")
                    .arg(keys)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(redis_error)?;
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        let payload = serde_json::to_vec(&BackendInvalidation::<T::Id>::All)?;
        conn.publish::<_, _, ()>(Self::channel(keyspace), payload)
            .await
            .map_err(redis_error)?;
        Ok(())
    }

    fn invalidation_stream(
        &self,
        keyspace: BackendKeyspace,
    ) -> BoxStream<'static, Result<BackendInvalidation<T::Id>, BackendError>> {
        let client = self.client.clone();
        let channel = Self::channel(&keyspace);
        Box::pin(async_stream::stream! {
            let mut reconnect_delay = INITIAL_INVALIDATION_RECONNECT_DELAY;
            loop {
                let mut pubsub = match client.get_async_pubsub().await {
                    Ok(pubsub) => pubsub,
                    Err(err) => {
                        yield Err(redis_error(err));
                        tokio::time::sleep(reconnect_delay).await;
                        reconnect_delay = next_invalidation_reconnect_delay(reconnect_delay);
                        continue;
                    }
                };
                if let Err(err) = pubsub.subscribe(channel.as_str()).await {
                    yield Err(redis_error(err));
                    tokio::time::sleep(reconnect_delay).await;
                    reconnect_delay = next_invalidation_reconnect_delay(reconnect_delay);
                    continue;
                }
                let mut messages = pubsub.into_on_message();
                while let Some(message) = messages.next().await {
                    match message.get_payload::<Vec<u8>>() {
                        Ok(raw) => match serde_json::from_slice::<BackendInvalidation<T::Id>>(&raw) {
                            Ok(invalidation) => {
                                reconnect_delay = INITIAL_INVALIDATION_RECONNECT_DELAY;
                                yield Ok(invalidation);
                            }
                            Err(err) => yield Err(BackendError::Serialization(err.to_string())),
                        },
                        Err(err) => yield Err(redis_error(err)),
                    }
                }
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = next_invalidation_reconnect_delay(reconnect_delay);
            }
        })
    }
}

fn next_invalidation_reconnect_delay(current: Duration) -> Duration {
    cmp::min(current.saturating_mul(2), MAX_INVALIDATION_RECONNECT_DELAY)
}

fn redis_error(err: redis::RedisError) -> BackendError {
    BackendError::Network(err.to_string())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}
