# Getting Started

Sassi's first useful shape is small: define a cacheable model, build a
`Punnu<T>`, insert values, read by id, and use an explicit scope when you want a
query-shaped view of resident data.

## Install

The default feature set enables `serde` and the native Tokio runtime path:

```toml
[dependencies]
sassi = "0.1.0-beta.2"
tokio = { version = "1", features = ["macros", "rt"] }
```

Inside this repository, path dependencies are relative to the crate that
declares them. From the repository root that looks like this:

```toml
[dependencies]
sassi = { path = "sassi" }
tokio = { version = "1", features = ["macros", "rt"] }
```

From a workspace member under `examples/`, use a relative path like
`../../sassi`.

## A Minimal Model

`#[derive(Cacheable)]` expects a struct with named fields and a field literally
named `id`. The generated companion fields let predicates inspect the same
field names and values that Sassi can evaluate in memory.

If a type is stored in a shared or durable L2 backend, add a stable
`type_name` so backend keys survive Rust module moves. The derive default is
meant for local caches, tests, and examples rather than long-lived shared
storage.

```rust
use sassi::{Cacheable, MemQ, Punnu};

#[derive(Cacheable, Clone, Debug)]
#[cacheable(type_name = "myapp.User")]
struct User {
    id: i64,
    name: String,
    active: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let users = Punnu::<User>::builder().build();

    users
        .insert(User {
            id: 1,
            name: "Asha".to_owned(),
            active: true,
        })
        .await?;

    let one = users.get(&1).expect("user should be resident");
    assert_eq!(one.name, "Asha");

    let active = users
        .scope(vec![MemQ::filter_basic(User::fields().active.eq(true))])
        .collect();
    assert_eq!(active.len(), 1);

    Ok(())
}
```

`get` is synchronous because it only checks the in-process L1 snapshot.
`insert` is async because the same API also supports async L2 write-through when
a backend is attached.

## Fetch On Miss

`get_or_fetch` is for canonical id fetches. On a hit, the fetcher is not called.
On a miss, the fetcher must return the value for the requested id or `None`.
If it returns a different id, Sassi rejects the result instead of contaminating
the identity map.

This snippet is marked `ignore` because it omits the application-specific
database or HTTP client that would load the user.

```rust,ignore
let user = users
    .get_or_fetch(&42, |id| async move {
        let row = load_user_by_id(id).await?;
        Ok::<_, sassi::FetchError>(row)
    })
    .await?;
```

Use `get_or_fetch_many` when the source naturally resolves a set of canonical
ids in one round trip. It is not a query/page helper; every returned id must be
one of the requested missing ids.

The returned vector is not input-order preserving: resident hits and newly
fetched values are combined according to cache/fetch order. Build a map by id
when the caller needs positional lookup.

## Feature Selection

`serde` enables the binary wire container and `CacheBackend` integration.
Disable it only when you want an L1-only in-process cache with the smallest
surface:

```toml
sassi = { version = "0.1.0-beta.2", default-features = false }
```

`runtime-tokio` is the native background-work path. It is selected by default
and is needed when native builds use backend invalidation streams, background
TTL sweep, or refresh tasks.

`runtime-wasm` is the `wasm32-unknown-unknown` background-work path. It uses
`wasm-bindgen-futures` for spawning and `gloo-timers` for sleeps. Build it
explicitly for browser/WASM targets:

```toml
sassi = {
    version = "0.1.0-beta.2",
    default-features = false,
    features = ["serde", "runtime-wasm"],
}
```

`watermark-time` and `watermark-chrono` add `MonotonicWatermark` marker impls
for selected `time` and `chrono` timestamp types. They are useful when a delta
sync cursor is already represented by one of those libraries:

```toml
sassi = { version = "0.1.0-beta.2", features = ["watermark-time"] }
```

## Where Next

Read [Concepts](concepts.md) for the mental model, then
[Query And Refresh Boundaries](query-refresh-boundaries.md) before using
refreshers, tenant-specific data, auth-filtered rows, or paginated queries.
For Redis, file, memory, native, and WASM deployment notes, see
[Backends And Runtimes](backends-and-runtimes.md).
