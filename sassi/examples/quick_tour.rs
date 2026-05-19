//! Quick tour of the Sassi typed cache pool.
//!
//! Build the pool, insert a typed value, and query a local view using
//! composable predicates. The same shape appears in the crate-level
//! rustdoc (`sassi/src/lib.rs`) and in the lead `README.md`; this file
//! is the CI-verified source of truth.

use sassi::{Cacheable, MemQ, Punnu};

#[derive(sassi::Cacheable)]
struct User {
    id: i64,
    age: u32,
    is_active: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // 1. Build the in-memory pool and populate it.
    let users = Punnu::<User>::builder().build();
    users
        .insert(User {
            id: 1,
            age: 32,
            is_active: true,
        })
        .await
        .unwrap();

    // 2. Query local state with composable predicates.
    let adults = users
        .scope(vec![MemQ::filter_basic(
            User::fields().age.gte(18) & User::fields().is_active.eq(true),
        )])
        .take(10)
        .collect();
    assert_eq!(adults.len(), 1);
}
