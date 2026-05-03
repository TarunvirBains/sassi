# sassi-cache-redis

Redis `CacheBackend` implementation for Sassi.

This companion crate keeps Redis support outside the core `sassi` crate while
providing the L2 storage and pub/sub invalidation path that multi-process
deployments usually need.

```toml
[dependencies]
sassi = "0.1.0-alpha.1"
sassi-cache-redis = "0.1.0-alpha.1"
```

The backend uses the `BackendKeyspace` supplied by `PunnuConfig::namespace` and
the cached Rust type. It does not carry a separate namespace of its own.

See the Sassi repository docs for the broader cache model and release notes:

- https://github.com/TarunvirBains/sassi/blob/v0.1.0-alpha.1/docs/backends-and-runtimes.md
- https://github.com/TarunvirBains/sassi/blob/v0.1.0-alpha.1/docs/release-readiness.md
