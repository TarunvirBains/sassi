# Advanced Guide

This page collects practical patterns for adopters who have outgrown the
[Getting Started](getting-started.md) and [Concepts](concepts.md) tour and want
to use the more nuanced parts of the Sassi public surface confidently. Each
section pairs a short rationale with code that runs against the same
`Cacheable` shape used elsewhere in the docs. None of this is necessary on day
one; the examples here become useful when integrations grow.

## Predicate Walk Surface (`FieldPredicate`, `LookupOp`, `value_as`)

`BasicPredicate<T>` is meant to be both walkable and projectable. Any
downstream emitter — a SQL query builder, a debug formatter, a wire-format
codec — can inspect a predicate tree without re-running it through Sassi's
in-memory evaluator. The relevant types are:

- `BasicPredicate<T>`: the universal algebra. Variants: `True`, `False`,
  `Field`, `And`, `Or`, `Not`, `Xor`. `And` and `Or` flatten on construction so
  `a & b & c` produces one `And(vec![a, b, c])`.
- `FieldPredicate<T>`: the per-field payload carried inside
  `BasicPredicate::Field`. Exposes `field_name()`, `op() -> LookupOp`, and the
  type-erased operand value.
- `LookupOp`: the operator marker. Tells walkers what shape the operand value
  carries (e.g. `Eq` carries `V`, `Between` carries `(V, V)`, `In` carries
  `Vec<V>`, `IsNull` carries `()`).

`FieldPredicate::value_as::<V>()` does the typed downcast from the `Arc<dyn
Any>` payload. The caller must know `V` at compile time. That is enough for
SQL-emitting walkers that hold a generated field-type registry; it is not
enough for generic predicate persistence across processes that do not share a
type registry.

```rust
use sassi::{BasicPredicate, Cacheable, Field, LookupOp};

#[derive(Cacheable, Clone, Debug)]
struct User {
    id: i64,
    age: u32,
    role: String,
}

fn describe(predicate: &BasicPredicate<User>) -> String {
    match predicate {
        BasicPredicate::Field(fp) => match fp.op() {
            LookupOp::Eq => {
                if let Some(role) = fp.value_as::<String>() {
                    format!("{} == {role:?}", fp.field_name())
                } else if let Some(age) = fp.value_as::<u32>() {
                    format!("{} == {age}", fp.field_name())
                } else {
                    format!("{} == <unknown type>", fp.field_name())
                }
            }
            LookupOp::Gte => {
                if let Some(age) = fp.value_as::<u32>() {
                    format!("{} >= {age}", fp.field_name())
                } else {
                    format!("{} >= <unknown type>", fp.field_name())
                }
            }
            other => format!("{:?}({})", other, fp.field_name()),
        },
        BasicPredicate::And(children) => {
            let parts: Vec<_> = children.iter().map(describe).collect();
            format!("AND({})", parts.join(", "))
        }
        BasicPredicate::Or(children) => {
            let parts: Vec<_> = children.iter().map(describe).collect();
            format!("OR({})", parts.join(", "))
        }
        BasicPredicate::Not(inner) => format!("NOT({})", describe(inner)),
        BasicPredicate::Xor(a, b) => format!("XOR({}, {})", describe(a), describe(b)),
        BasicPredicate::True => "TRUE".to_owned(),
        BasicPredicate::False => "FALSE".to_owned(),
    }
}

let predicate = User::fields().age.gte(18) & User::fields().role.eq("admin".to_owned());
let description = describe(&predicate);
assert!(description.contains("AND"));
```

`LookupOp` is `#[non_exhaustive]`. Walkers that match on it should always
include a default arm so future ops can be added without breaking downstream
crates.

## JSahibON Predicate Payloads

JSON predicates are ordinary `BasicPredicate<T>` values with
`LookupOp::Json`. The operand downcasts to `JSahibONPredicateBody`, which keeps
the JSON path and predicate body inspectable for downstream query emitters and
debug formatters.

```rust
use sassi::{BasicPredicate, Cacheable, JSahibON, JSahibONPredicateBody, LookupOp};

#[derive(Cacheable, Clone, Debug)]
struct Event {
    id: i64,
    payload: JSahibON,
}

fn describe_json(predicate: &BasicPredicate<Event>) -> Option<String> {
    let BasicPredicate::Field(fp) = predicate else {
        return None;
    };
    if fp.op() != LookupOp::Json {
        return None;
    }

    let body = fp.value_as::<JSahibONPredicateBody>()?;
    let path = |segments: &[String]| {
        if segments.is_empty() {
            "$".to_owned()
        } else {
            segments.join(".")
        }
    };

    Some(match body {
        JSahibONPredicateBody::HasKey { path: jpath, key } => {
            format!("{}.{} has key {:?}", fp.field_name(), path(jpath.segments()), key)
        }
        JSahibONPredicateBody::ScalarCompare { path: jpath, op, operand, .. } => {
            format!("{}.{} {:?} {:?}", fp.field_name(), path(jpath.segments()), op, operand)
        }
        other => format!("{} JSON {:?}", fp.field_name(), other),
    })
}

let predicate = Event::fields().payload.jsahibon().path("profile").has_key("age");
assert!(describe_json(&predicate).unwrap().contains("has key"));
```

`JSahibONPredicateBody` is `#[non_exhaustive]`, so walkers should keep a
fallback arm. Dotted `path("profile.age")` segments are already parsed into a
`JPath`; literal keys reached through `key(...)` or `path_segments(...)` appear
as exact string segments with no escaping layer.

## `PunnuScope` Chaining And Terminal Collection

`PunnuScope<T>` is the lazy in-memory query handle returned by `Punnu::scope`.
It owns a cheap clone of the pool plus a `Vec<MemQ<T>>`. Operations are
appended; nothing runs until a terminal method is called.

Terminal methods:

- `collect()` — `Vec<Arc<T>>`. The default; use this when you need an owned
  list of results.
- `iter()` — consuming `IntoIter<Arc<T>>`. Same data, different shape.
- `first()` — `Option<Arc<T>>`. Equivalent to `take(1).collect().pop()`.
- `count()` — `usize`. Forces full evaluation but throws away the values.
- `exists()` — `bool`. Short-circuits on the first match (uses `first()`).

```rust
use sassi::{Cacheable, MemQ, Punnu};

#[derive(Cacheable, Clone, Debug)]
struct Order {
    id: i64,
    region: String,
    total_cents: u64,
}

# let pool = Punnu::<Order>::builder().build();
let high_value_in_eu = pool
    .scope(Vec::<MemQ<Order>>::new())
    .filter_basic(|fields| fields.region.eq("eu".to_owned()))
    .filter_basic(|fields| fields.total_cents.gte(50_000))
    .sort_by_key(|order| order.total_cents)
    .take(10)
    .collect();
```

`then(MemQ<T>)` is the escape hatch when you have already built a `MemQ<T>`
elsewhere — for example, when a service constructs reusable filter pipelines:

```rust,ignore
let base = vec![MemQ::filter_basic(Order::fields().region.eq("eu".to_owned()))];
let scope = pool.scope(base).then(MemQ::take(50));
```

`PunnuScope` is intentionally unbounded in the operations it accepts. Order
matters — `take(10).filter_closure(...)` will only filter from the first 10
entries snapshotted, while `filter_closure(...).take(10)` filters first then
takes ten. Sassi does no automatic reordering.

## `MemQ` Terminal Helpers

`MemQ::apply_all(&[ops], entries)` is the underlying terminal evaluator. Most
adopters reach it indirectly through `PunnuScope::collect`, but the helper is
public so library code can compose pipelines from caller-owned data:

```rust,ignore
use sassi::{Cacheable, MemQ};

let snapshot: Vec<std::sync::Arc<Order>> = source.snapshot();
let filtered = MemQ::apply_all(
    &[
        MemQ::filter_basic(Order::fields().region.eq("eu".to_owned())),
        MemQ::take(5),
    ],
    snapshot,
);
```

`MemQ::filter` takes a closure (`Fn(&T) -> bool`); `MemQ::filter_basic` takes
anything that implements `IntoBasicPredicate<T>`. Choose `filter_basic` when
the predicate could in principle be lowered to a remote query later — its
walkable representation survives any subsequent scope inspection. Choose
`filter` when the matcher captures runtime state (timestamps, regex compiled at
construction, request-scoped allow-lists) and the predicate is local-only by
design.

The full `MemQ` set: `filter`, `map`, `flat_map`, `take`, `skip`, `chain`,
`sort`, `sort_by_key`, `unique`, `unique_by`, `group_by`, `partition`, `fold`.
All enum variants are `#[non_exhaustive]`; build them through the constructors.

## `#[trait_impl]` Registry Behavior And Accessor Visibility

`#[sassi::trait_impl]` registers a `(model type, trait)` pair into a shared
inventory at process startup. The `Sassi` orchestrator then exposes
`Sassi::all_impl::<dyn Trait>()`, which returns one `Arc<dyn Trait>` per
registered model that has been registered with the orchestrator.

Properties worth knowing:

- The trait must be object-safe (`dyn Trait`) and bounded
  `Send + Sync + 'static`. Sassi's macro emits a static check; if the bound is
  missing, expansion fails with a compile-time error.
- Registration is idempotent per `(TypeId(model), TypeId(trait))`. Re-importing
  a crate that registers the same pair does not duplicate.
- Accessor visibility tracks the impl's visibility: `pub trait` impls are
  visible to any consumer; private trait impls register but are only callable
  within the crate that owns the trait.
- `inventory` handles the platform link sections. WASM is supported; no extra
  setup is required.

```rust
use std::sync::Arc;

trait Greeter: Send + Sync {
    fn greet(&self) -> String;
}

#[derive(sassi::Cacheable, Clone)]
struct User {
    id: i64,
    name: String,
}

#[sassi::trait_impl]
impl Greeter for User {
    fn greet(&self) -> String {
        format!("hi, {}", self.name)
    }
}

# let users = sassi::Punnu::<User>::builder().build();
let mut orchestrator = sassi::Sassi::new();
orchestrator.register::<User>(Arc::new(users));

let greeters: Vec<Arc<dyn Greeter>> = orchestrator.all_impl::<dyn Greeter>();
for g in &greeters {
    let _ = g.greet();
}
```

Re-registering the same `T` replaces the prior pool for that `TypeId` in the
orchestrator. The trait registry itself is not affected — it is keyed by
`(TypeId(model), TypeId(trait))` and lives at process scope.

## Delta Refresh Handle Operations And Recovery

`start_delta_refresh` returns a `DeltaRefreshHandle<T>` that owns the
subscription's watermark, recovery queue, and single-flight slot. The handle
is the only way to drive a delta subscription manually; dropping it stops the
background task.

Operations:

- `update()` runs one delta tick. Uses the current watermark; the fetcher
  receives `since = Some(watermark)` (or `None` for the first tick).
- `update_full()` forces a full refresh: the fetcher gets `since = None` and
  is responsible for returning the full authoritative set. If a delta tick is
  already in flight, the full refresh queues behind it.
- `watermark()` returns the current watermark cursor; useful for diagnostics
  or for handing off progress to a parallel subscription.
- `pending_eviction_recovery_count()` reports how many ids the recovery queue
  is waiting to refetch. Recovery happens when an entry is evicted by the
  receiving pool while a subscription is still interested in it.
- `periodic_full_refresh_progress()` reports how far the subscription is into
  its periodic full-refresh window when one is configured.

Modifiers (chaining-style):

- `with_eviction_recovery(true)` — enable best-effort refetch of evicted ids
  next tick.
- `with_periodic_full_refresh(Some(NonZeroUsize))` — after every N delta ticks
  run a full refresh to recover from cursor drift.

```rust,ignore
use std::num::NonZeroUsize;
use std::time::Duration;

let handle = users
    .start_delta_refresh(Duration::from_secs(15), fetcher)
    .with_eviction_recovery(true)
    .with_periodic_full_refresh(Some(NonZeroUsize::new(60).unwrap()));

if handle.pending_eviction_recovery_count() > 0 {
    let _ = handle.update().await?;
}
```

Recovery policy is best-effort: the subscription tries to refetch lost ids,
but a fetcher that consistently returns `None` for the lost id can starve the
recovery queue. Watch `pending_eviction_recovery_count()` and
`periodic_full_refresh_progress()` together when wiring observability.

The watermark/refresh handle state is intentionally not part of
`Punnu::snapshot_postcard` (in either mode). The handle owns its progress; the
pool owns the resident union. After a process restart that restores entries
from a snapshot, re-attach a fresh refresh handle and let the watermark
resume from the consumer's persisted cursor.

## Backend Implementer Examples

`CacheBackend<T>` is small. A custom backend can provide the four required
methods plus the optional `invalidation_stream`:

```rust,ignore
use async_trait::async_trait;
use futures::stream;
use sassi::{
    BackendError, BackendInvalidation, BackendInvalidationStream, BackendKeyspace,
    CacheBackend,
};
use std::sync::Mutex;
use std::time::Duration;

struct InMemoryEcho<T> {
    inner: Mutex<std::collections::HashMap<String, Vec<u8>>>,
    _t: std::marker::PhantomData<T>,
}

#[async_trait]
impl<T> CacheBackend<T> for InMemoryEcho<T>
where
    T: sassi::Cacheable + serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    T::Id: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    async fn get(
        &self,
        keyspace: &BackendKeyspace,
        id: &T::Id,
    ) -> Result<Option<T>, BackendError> {
        let key = format!("{:?}/{:?}", keyspace.type_name, id);
        let bytes = self
            .inner
            .lock()
            .map_err(|e| BackendError::Other(e.to_string().into()))?
            .get(&key)
            .cloned();
        match bytes {
            Some(bytes) => Ok(Some(sassi::wire::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn put(
        &self,
        keyspace: &BackendKeyspace,
        id: &T::Id,
        value: &T,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let key = format!("{:?}/{:?}", keyspace.type_name, id);
        let bytes = sassi::wire::to_vec(value)?;
        self.inner
            .lock()
            .map_err(|e| BackendError::Other(e.to_string().into()))?
            .insert(key, bytes);
        Ok(())
    }

    async fn invalidate(
        &self,
        keyspace: &BackendKeyspace,
        id: &T::Id,
    ) -> Result<(), BackendError> {
        let key = format!("{:?}/{:?}", keyspace.type_name, id);
        self.inner
            .lock()
            .map_err(|e| BackendError::Other(e.to_string().into()))?
            .remove(&key);
        Ok(())
    }

    async fn invalidate_all(
        &self,
        _keyspace: &BackendKeyspace,
    ) -> Result<(), BackendError> {
        self.inner
            .lock()
            .map_err(|e| BackendError::Other(e.to_string().into()))?
            .clear();
        Ok(())
    }

    fn invalidation_stream(
        &self,
        _keyspace: BackendKeyspace,
    ) -> BackendInvalidationStream<T::Id> {
        // Default no-op stream is fine for backends without pub/sub.
        Box::pin(stream::empty())
    }
}
```

Useful invariants when implementing custom backends:

- `BackendKeyspace` is the only namespace/type identifier the backend should
  use. Encode it explicitly (e.g., hex-encode the type name and namespace
  bytes); do not introduce an independent backend-side namespace.
- `put` is the only async write the backend trait promises. `invalidate` and
  `invalidate_all` are publish-and-delete for the keyspace; they do not
  guarantee a quiescence barrier against concurrent writers.
- `invalidation_stream` is best-effort. Sassi's listener reconnects with
  capped exponential backoff if the stream errors. Backends without pub/sub
  can rely on the default empty implementation.
- `get` returning `None` means "absent or unknown" — the backend should not
  use `BackendError::NotFound` for ordinary cache misses unless its own
  storage semantics require distinguishing them.

For Redis specifically, see the
[`sassi-cache-redis` crate](https://crates.io/crates/sassi-cache-redis) — it
implements pub/sub invalidation, key-index sweeping, and Lua-coupled mutation
on top of the `CacheBackend` trait.

## Snapshot/Restore Modes

`Punnu::snapshot_postcard(mode)` is the whole-pool snapshot wrapper. The
default mode (`SnapshotMode::EntriesOnly`) writes the same byte stream as
`export_entries_postcard` — values only, no per-entry hints. The opt-in
`SnapshotMode::WithInternalState` mode additionally serializes per-entry
remaining TTL and a relative sampled-LRU recency rank so the receiving pool
can preserve eviction priority and freshness across the boundary.

```rust,ignore
use sassi::SnapshotMode;

let bytes = pool.snapshot_postcard(SnapshotMode::default())?;
restored.restore_postcard(&bytes)?;

let with_hints = pool.snapshot_postcard(SnapshotMode::WithInternalState)?;
restored.restore_postcard(&with_hints)?;
```

`restore_postcard` auto-dispatches on the wire kind byte. The kind discriminator
is independent of the wire major version, and the with-hints body carries its
own envelope version (`__sassi_punnu_v` semantically) so internal-state
evolution does not require a wire-major bump that would also reject
entries-only snapshots.

What is *not* serialized in either mode:

- Executor handles (`runtime-tokio` / `runtime-wasm` are process-local).
- Backend handles, backend invalidation listeners, and backend strict-insert
  reservations.
- Single-flight in-flight registry entries (active fetcher futures).
- Event broadcast subscribers.
- `DeltaRefreshHandle` and `RefreshHandle` state. Refresh handles are owned
  by the consumer that called `start_periodic_refresh` / `start_delta_refresh`;
  they hold their own watermark, recovery queue, and single-flight slot which
  the pool itself does not own. After a restore, re-attach refresh handles
  and let the watermark resume from the consumer's persisted cursor.

The legacy `export_entries_postcard` / `restore_entries_postcard` methods
remain available and are wire-compatible with the wrapper's `EntriesOnly`
mode.
