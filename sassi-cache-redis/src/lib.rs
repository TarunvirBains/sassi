//! Redis [`sassi::CacheBackend`] companion crate.

use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use redis::AsyncCommands;
use sassi::{BackendError, BackendInvalidation, BackendKeyspace, CacheBackend, Cacheable};
use serde::{Serialize, de::DeserializeOwned};
use std::cmp;
use std::marker::PhantomData;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const INITIAL_INVALIDATION_RECONNECT_DELAY: Duration = Duration::from_millis(10);
const MAX_INVALIDATION_RECONNECT_DELAY: Duration = Duration::from_secs(5);
const PERSISTENT_INDEX_SCORE: i64 = 9_007_199_254_740_991;
const PUT_WITH_INDEX_SCRIPT: &str = r#"
local mode = ARGV[1]
local persistent_score = tonumber(ARGV[2])
local now_parts = redis.call("TIME")
local now_ms = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)

local function prune_expired_index_members()
    redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now_ms)
end

local function refresh_index_expiry()
    local max = redis.call("ZREVRANGE", KEYS[2], 0, 0, "WITHSCORES")
    if max[1] == nil then
        redis.call("DEL", KEYS[2])
        return
    end
    local score = tonumber(max[2])
    if score >= persistent_score then
        redis.call("PERSIST", KEYS[2])
    else
        redis.call("PEXPIREAT", KEYS[2], math.floor(score))
    end
end

prune_expired_index_members()

if mode == "delete" then
    redis.call("DEL", KEYS[1])
    redis.call("ZREM", KEYS[2], KEYS[1])
    refresh_index_expiry()
    return 1
end
if mode == "set" then
    redis.call("SET", KEYS[1], ARGV[3])
    redis.call("ZADD", KEYS[2], persistent_score, KEYS[1])
    refresh_index_expiry()
    return 1
end
if mode == "psetex" then
    local ttl_ms = tonumber(ARGV[3])
    redis.call("PSETEX", KEYS[1], ttl_ms, ARGV[4])
    redis.call("ZADD", KEYS[2], now_ms + ttl_ms, KEYS[1])
    refresh_index_expiry()
    return 1
end
return redis.error_reply("unsupported RedisBackend put mode")
"#;
const GET_WITH_INDEX_PRUNE_SCRIPT: &str = r#"
local persistent_score = tonumber(ARGV[1])
local now_parts = redis.call("TIME")
local now_ms = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)

local function refresh_index_expiry()
    local max = redis.call("ZREVRANGE", KEYS[2], 0, 0, "WITHSCORES")
    if max[1] == nil then
        redis.call("DEL", KEYS[2])
        return
    end
    local score = tonumber(max[2])
    if score >= persistent_score then
        redis.call("PERSIST", KEYS[2])
    else
        redis.call("PEXPIREAT", KEYS[2], math.floor(score))
    end
end

redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now_ms)
local raw = redis.call("GET", KEYS[1])
if raw == false then
    redis.call("ZREM", KEYS[2], KEYS[1])
end
refresh_index_expiry()
return raw
"#;
const INVALIDATE_ONE_SCRIPT: &str = r#"
local persistent_score = tonumber(ARGV[1])

local function refresh_index_expiry()
    local max = redis.call("ZREVRANGE", KEYS[2], 0, 0, "WITHSCORES")
    if max[1] == nil then
        redis.call("DEL", KEYS[2])
        return
    end
    local score = tonumber(max[2])
    if score >= persistent_score then
        redis.call("PERSIST", KEYS[2])
    else
        redis.call("PEXPIREAT", KEYS[2], math.floor(score))
    end
end

redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], KEYS[1])
refresh_index_expiry()
redis.call("PUBLISH", ARGV[2], ARGV[3])
return 1
"#;
const INVALIDATE_ALL_BATCH_SCRIPT: &str = r#"
local persistent_score = tonumber(ARGV[1])
for i = 2, #KEYS do
    redis.call("DEL", KEYS[i])
    redis.call("ZREM", KEYS[1], KEYS[i])
end
local max = redis.call("ZREVRANGE", KEYS[1], 0, 0, "WITHSCORES")
if max[1] == nil then
    redis.call("DEL", KEYS[1])
elseif tonumber(max[2]) >= persistent_score then
    redis.call("PERSIST", KEYS[1])
else
    redis.call("PEXPIREAT", KEYS[1], math.floor(tonumber(max[2])))
end
return #KEYS - 1
"#;

/// Redis-backed Sassi L2 backend.
///
/// Keys and pub/sub channels are derived only from Sassi's
/// [`BackendKeyspace`]. The backend carries no independent namespace,
/// so `PunnuConfig::namespace` remains the single source of truth.
///
/// `invalidate_all` deletes keys recorded in the backend-maintained key index
/// and publishes one keyspace-wide invalidation message. It is scoped by
/// namespace and `Cacheable::cache_type_name()`, but it is not a quiescence
/// barrier against concurrent writers in the same keyspace. Value/index updates
/// use Redis Lua scripts so each data-key mutation and its index mutation are
/// applied as one Redis operation. Read and write operations also prune expired
/// TTL index members.
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
        format!("sassi:{{{namespace}:{type_part}}}")
    }

    fn channel(keyspace: &BackendKeyspace) -> String {
        format!("{}:invalidate", Self::key_prefix(keyspace))
    }

    fn key_index(keyspace: &BackendKeyspace) -> String {
        format!("{}:keys", Self::key_prefix(keyspace))
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
        let key_index = Self::key_index(keyspace);
        let raw: Option<Vec<u8>> = redis::Script::new(GET_WITH_INDEX_PRUNE_SCRIPT)
            .key(&key)
            .key(&key_index)
            .arg(PERSISTENT_INDEX_SCORE)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_error)?;
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
        let key_index = Self::key_index(keyspace);
        let script = redis::Script::new(PUT_WITH_INDEX_SCRIPT);
        let redis_ttl = match ttl {
            Some(ttl) => Some(redis_ttl_millis(ttl, unix_now_millis()?)?),
            None => None,
        };
        match redis_ttl {
            Some(0) => {
                let _: i32 = script
                    .key(&key)
                    .key(&key_index)
                    .arg("delete")
                    .arg(PERSISTENT_INDEX_SCORE)
                    .invoke_async(&mut conn)
                    .await
                    .map_err(redis_error)?;
            }
            Some(millis) => {
                let _: i32 = script
                    .key(&key)
                    .key(&key_index)
                    .arg("psetex")
                    .arg(PERSISTENT_INDEX_SCORE)
                    .arg(millis)
                    .arg(bytes)
                    .invoke_async(&mut conn)
                    .await
                    .map_err(redis_error)?;
            }
            None => {
                let _: i32 = script
                    .key(&key)
                    .key(&key_index)
                    .arg("set")
                    .arg(PERSISTENT_INDEX_SCORE)
                    .arg(bytes)
                    .invoke_async(&mut conn)
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
        let key_index = Self::key_index(keyspace);
        let payload = serde_json::to_vec(&BackendInvalidation::Id(id.clone()))?;
        let _: i32 = redis::Script::new(INVALIDATE_ONE_SCRIPT)
            .key(&key)
            .key(&key_index)
            .arg(PERSISTENT_INDEX_SCORE)
            .arg(Self::channel(keyspace))
            .arg(payload)
            .invoke_async(&mut conn)
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
        let key_index = Self::key_index(keyspace);
        loop {
            let keys: Vec<String> = redis::cmd("ZRANGE")
                .arg(&key_index)
                .arg(0_isize)
                .arg(499_isize)
                .query_async(&mut conn)
                .await
                .map_err(redis_error)?;
            if keys.is_empty() {
                break;
            }
            let script = redis::Script::new(INVALIDATE_ALL_BATCH_SCRIPT);
            let mut invocation = script.key(&key_index);
            for key in &keys {
                invocation.key(key);
            }
            invocation.arg(PERSISTENT_INDEX_SCORE);
            let _: i32 = invocation
                .invoke_async(&mut conn)
                .await
                .map_err(redis_error)?;
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

fn unix_now_millis() -> Result<u128, BackendError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| BackendError::Other(err.into()))?
        .as_millis())
}

fn redis_ttl_millis(ttl: Duration, now_ms: u128) -> Result<u64, BackendError> {
    if ttl.is_zero() {
        return Ok(0);
    }

    let ttl_ms = ttl.as_millis().max(1);
    let max_redis_ttl_ms = (i64::MAX as u128)
        .saturating_sub(now_ms)
        .saturating_sub(1_000);
    if ttl_ms > max_redis_ttl_ms {
        Err(BackendError::Other(
            "Redis TTL exceeds configured Redis absolute time window; pass a smaller duration or no TTL".into(),
        ))
    } else {
        Ok(ttl_ms as u64)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn invalidation_reconnect_delay_caps_at_outage_scale_interval() {
        let mut delay = INITIAL_INVALIDATION_RECONNECT_DELAY;
        for _ in 0..16 {
            delay = next_invalidation_reconnect_delay(delay);
        }

        assert_eq!(delay, Duration::from_secs(5));
        assert_eq!(
            next_invalidation_reconnect_delay(Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn invalidate_all_batch_script_prunes_scanned_members_without_scanning_mutated_set() {
        assert!(INVALIDATE_ALL_BATCH_SCRIPT.contains(r#"redis.call("ZREM", KEYS[1], KEYS[i])"#));
        let empty_branch = INVALIDATE_ALL_BATCH_SCRIPT
            .find("if max[1] == nil")
            .expect("script should delete the index only after proving it is empty");
        let index_delete = INVALIDATE_ALL_BATCH_SCRIPT
            .find(r#"redis.call("DEL", KEYS[1])"#)
            .expect("script should clean up an empty index key");
        assert!(index_delete > empty_branch);
        assert!(!INVALIDATE_ALL_BATCH_SCRIPT.contains("SSCAN"));
    }

    #[test]
    fn keyspace_keys_share_a_redis_cluster_hash_tag() {
        let keyspace = BackendKeyspace {
            namespace: Some(Arc::from("tenant-a")),
            type_name: "myapp.User",
        };
        let prefix = RedisBackend::<()>::key_prefix(&keyspace);
        let index = RedisBackend::<()>::key_index(&keyspace);
        let channel = RedisBackend::<()>::channel(&keyspace);

        assert!(prefix.starts_with("sassi:{"));
        let tag = prefix
            .split_once('{')
            .and_then(|(_, rest)| rest.split_once('}'))
            .map(|(tag, _)| tag)
            .expect("prefix should contain a Redis Cluster hash tag");
        assert!(index.contains(&format!("{{{tag}}}")));
        assert!(channel.contains(&format!("{{{tag}}}")));
    }

    #[test]
    fn redis_ttl_normalization_preserves_public_punnu_ttl_contract() {
        let now_ms = 1_800_000_000_000_u128;

        assert_eq!(
            redis_ttl_millis(Duration::from_nanos(1), now_ms).unwrap(),
            1
        );
        assert_eq!(redis_ttl_millis(Duration::ZERO, now_ms).unwrap(), 0);
        assert!(redis_ttl_millis(Duration::MAX, now_ms).is_err());
    }
}
