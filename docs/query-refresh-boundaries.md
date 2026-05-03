# Query And Refresh Boundaries

The most important Sassi habit is to keep identity separate from per-query
inclusion. A `Punnu<T>` stores resident canonical values by id. Queries,
tenants, auth rules, pagination, and refresh progress belong in explicit
predicates, wrapper types, ids, or refresh subscriptions.

## Resident Union Model

A `Punnu<T>` is a resident union identity map. If two refreshers both return
`User { id: 7, ... }`, they are talking about the same cached identity. The pool
does not remember which query first loaded that user, nor does it maintain a
private membership set for every read scope.

That model keeps memory use and coherence tractable. It also means membership
has to be represented deliberately. Read scopes are safe when they filter the
current resident union:

```rust
use sassi::{Cacheable, MemQ, Punnu};

#[derive(Cacheable, Clone, Debug)]
struct User {
    id: i64,
    team: String,
    active: bool,
}

fn active_ops_users(users: &Punnu<User>) -> Vec<std::sync::Arc<User>> {
    users
        .scope(vec![
            MemQ::filter_basic(User::fields().active.eq(true)),
            MemQ::filter_basic(User::fields().team.eq("ops".to_owned())),
        ])
        .collect()
}
```

This is safe because the scope describes a read over whatever canonical users
are currently resident. It does not imply that Sassi has fetched every active
ops user from the source of truth.

## Canonical Identity Fetches

`get_or_fetch` and `get_or_fetch_many` are id helpers. They are for "ensure the
cache has this canonical identity", not "load this query page".

Safe shape:

```rust,ignore
let user = users
    .get_or_fetch(&user_id, |id| async move {
        Ok::<_, sassi::FetchError>(load_user_by_primary_key(id).await?)
    })
    .await?;
```

This snippet is marked `ignore` because it omits the application-specific data
client. The important contract is that the fetcher resolves the requested
canonical id.

Unsafe pattern:

```rust,ignore
let page = users
    .get_or_fetch_many(&visible_ids, move |_missing| async move {
        load_current_user_visible_page(auth_context, page_cursor).await
    })
    .await?;
```

This is unsafe as a Sassi pattern because the fetcher is using the batch API as
a paginated or auth-filtered query helper. It may return a subset shaped by the
current actor rather than by canonical identity. Put that boundary in the type,
the id, or a refresher that owns the query state.

## Periodic Refresh

Periodic refreshers are useful when the source can be polled cheaply. The
fetcher owns the query it is polling; the `Punnu<T>` only sees values to apply.

Use `RefreshMode::UpsertOnly` for partial refreshers. It inserts or updates
fetched rows and leaves other resident rows untouched.

Use `RefreshMode::Replace` only for a complete authoritative set for the whole
pool. If a fetcher returns "all currently visible tasks for user A" and uses
`Replace` against a shared `Punnu<Task>`, it can evict tasks that are still
valid for user B or another query.

This example is marked `ignore` because it omits the concrete
`PunnuFetcher<Task>` implementation.

```rust,ignore
let handle = tasks.start_periodic_refresh(
    std::time::Duration::from_secs(30),
    AllTasksFetcher::new(client.clone()),
    sassi::RefreshMode::Replace,
);
```

That shape is safe only if `AllTasksFetcher` returns the whole authoritative
task set for the `tasks` pool.

## Delta Refresh

Delta refresh subscriptions own their own watermark. Multiple subscriptions can
feed one resident union, but one subscription's progress does not advance
another's.

Fetchers must use inclusive `>= since` boundaries and return boundary rows
again. Sassi deduplicates by identity. This is a practical hedge against source
systems where the row changed but the visible watermark did not advance in the
way a strict `>` cursor expects.

Delta results have two meanings:

- `items`: upsert these canonical values.
- `tombstones`: delete these identities from the resident pool because the
  source of truth deleted them.

Do not use a tombstone to mean "this row stopped matching my filter". Return
the updated row and let read predicates stop selecting it.

## Tombstones And Soft Deletes

Hard deletes map naturally to tombstones. Soft deletes usually do not.

If the source keeps a row with `deleted_at`, `archived`, or `status = "closed"`,
return the updated row. Then scopes can filter it out:

```rust
use sassi::{Cacheable, MemQ, Punnu};

#[derive(Cacheable, Clone, Debug)]
struct Ticket {
    id: i64,
    archived: bool,
}

fn open_tickets(tickets: &Punnu<Ticket>) -> Vec<std::sync::Arc<Ticket>> {
    tickets
        .scope(vec![MemQ::filter_basic(
            Ticket::fields().archived.eq(false),
        )])
        .collect()
}
```

Use tombstones when the identity should leave the shared resident map entirely.

## Auth/Tenancy/RLS

Sassi does not infer tenant, auth, or row-level-security rules from cached
values. If those boundaries are correctness boundaries, model them directly.

Safe wrapper/id shape:

```rust
use sassi::Cacheable;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TenantId(String);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct UserId(i64);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TenantUserKey {
    tenant: TenantId,
    user: UserId,
}

#[derive(Cacheable, Clone, Debug)]
struct TenantUser {
    id: TenantUserKey,
    display_name: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TenantReportKey {
    tenant: TenantId,
    query_fingerprint: String,
}

#[derive(Cacheable, Clone, Debug)]
struct TenantReport {
    id: TenantReportKey,
    open_ticket_count: u64,
}
```

Now two tenants can both have user `1` without colliding, because the cache key
is `(tenant, user)`. `TenantReport` applies the same rule to a query-specific
aggregate cache: the aggregate's identity is the tenant plus the query
fingerprint, not a sentinel value inside the base model's pool.

`PunnuConfig::namespace` is still useful for L2 backend keyspace separation:
production versus staging, tenant-specific backend prefixes, or parallel test
runs. It does not isolate the L1 of a shared `Punnu<T>`.

## RefreshMode::Replace

`RefreshMode::Replace` removes resident ids absent from the fetched result. That
is sharp by design: it is the right operation for "this fetch returned the
complete authoritative set", and the wrong operation for "this fetch returned
one slice of a broader world".

Unsafe partial subscription:

```rust,ignore
let handle = tasks.start_periodic_refresh(
    std::time::Duration::from_secs(10),
    TasksVisibleToCurrentUser::new(auth_context),
    sassi::RefreshMode::Replace,
);
```

This is unsafe because a partial auth-filtered fetch can delete valid resident
tasks that were loaded by another actor or query. Prefer `UpsertOnly`, a
tenant/auth-qualified wrapper type, or a separate pool whose identity really is
the visible-query result.

## Practical Patterns

Use one shared `Punnu<T>` when every fetcher agrees on the canonical meaning of
`T::Id` and readers can express membership with predicates.

Use a tenant-qualified id when the same source id can exist under multiple
tenants and must not collide.

Use a wrapper type for aggregates, projections, or query-specific result
objects. A wrapper keeps base entities canonical while giving the aggregate its
own identity map.

Use `UpsertOnly` for partial pollers. Use `Replace` only when absence from the
fetch means absence from the whole pool.

Use tombstones for true source deletes. Use updated rows plus predicates for
soft deletes or "left this query" transitions.
