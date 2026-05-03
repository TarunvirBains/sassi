# sassi-cache-redis

Redis `CacheBackend` implementation for Sassi.

This companion crate keeps Redis support outside the core `sassi` crate while
providing shared L2 storage plus an explicit invalidation pub/sub path.

```toml
[dependencies]
sassi = "0.1.0-alpha.1"
sassi-cache-redis = "0.1.0-alpha.1"
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

`invalidate_all` is namespace/type scoped, but it is not a global write barrier
against concurrent writers in the same keyspace.

See the Sassi repository docs for the broader cache model and release notes:

- https://github.com/TarunvirBains/sassi/blob/v0.1.0-alpha.1/docs/backends-and-runtimes.md
- https://github.com/TarunvirBains/sassi/blob/v0.1.0-alpha.1/docs/release-readiness.md
