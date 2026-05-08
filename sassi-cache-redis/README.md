# sassi-cache-redis

Redis `CacheBackend` implementation for Sassi.

This companion crate keeps Redis support outside the core `sassi` crate while
providing shared L2 storage plus an explicit invalidation pub/sub path.

```toml
[dependencies]
sassi = "0.1.0-beta.2"
sassi-cache-redis = "0.1.0-beta.2"
```

The backend uses the `BackendKeyspace` supplied by `PunnuConfig::namespace` and
`Cacheable::cache_type_name()`. It does not carry a separate namespace of its
own.

Writes (`put`) are stored in Redis for the given keyspace, subject to the Redis
deployment's own persistence and eviction configuration, but they do not by
themselves publish invalidation messages. Multi-process visibility for those
writes comes from explicit `invalidate`/`invalidate_all` calls, or from your own
write coordination.

`invalidate_all` walks a per-keyspace index of keys written through
`RedisBackend`, so it does not scan the whole Redis database. Value/index
mutations are applied with Redis Lua scripts, and `invalidate_all` removes only
drained index members rather than deleting the whole index. Redis keys for one
Sassi namespace/type share a Cluster hash tag so multi-key scripts stay in one
slot. TTL-only index entries expire with their data keys, and reads prune
expired TTL index members in mixed persistent/TTL keyspaces.

The Lua scripts call Redis `TIME` to score TTL-backed index members. For
replicated Redis deployments, test this against the Redis version and
replication mode you operate; Redis 7.x deployments are the clearest path for
script effects replication.

`invalidate_all` is namespace/type scoped, but it is not a global write barrier
against concurrent writers in the same keyspace. A value written during the
drain can survive in Redis and be read back later. Coordinate writers outside
Sassi or move to a new namespace when a deployment needs a true generation
boundary.

`invalidate_all` may also partially succeed. It deletes Redis entries in
committed batches before publishing the final keyspace-wide invalidation
message. If that final publish fails, deleted Redis entries are not rolled back,
and other processes can retain stale L1 entries until a later invalidation,
refresh, restart, or namespace/generation rollover.

See the Sassi repository docs for the broader cache model and release notes:

- https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/docs/backends-and-runtimes.md
- https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/docs/release-readiness.md
