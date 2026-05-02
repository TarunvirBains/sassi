//! Integration tests for the in-memory [`sassi::MemQ`] extension
//! algebra and the owned [`sassi::PunnuScope`] query handle.

use sassi::predicate::MemQ;
use sassi::{Cacheable, Field, Punnu};
use std::future::Future;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
struct User {
    id: i64,
    age: u32,
    score: i32,
    team: String,
}

#[derive(Default)]
struct UserFields {
    age: Field<User, u32>,
    team: Field<User, String>,
}

impl Cacheable for User {
    type Id = i64;
    type Fields = UserFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> UserFields {
        UserFields {
            age: Field::new("age", |u| &u.age),
            team: Field::new("team", |u| &u.team),
        }
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime should build")
        .block_on(future)
}

fn user(id: i64, age: u32, score: i32, team: &str) -> User {
    User {
        id,
        age,
        score,
        team: team.to_owned(),
    }
}

fn arcs(values: Vec<User>) -> Vec<Arc<User>> {
    values.into_iter().map(Arc::new).collect()
}

#[test]
fn memq_sequence_ops_transform_lazily_on_apply() {
    let values = arcs(vec![
        user(1, 30, 2, "sales"),
        user(2, 17, 3, "sales"),
        user(3, 40, 1, "ops"),
    ]);

    let out = MemQ::apply_all(
        &[
            MemQ::filter(|u: &User| u.age >= 18),
            MemQ::map(|u: &User| user(u.id, u.age, u.score + 10, &u.team)),
            MemQ::flat_map(|u: &User| vec![u.clone()]),
            MemQ::chain_values(vec![user(1, 99, 99, "sales"), user(4, 22, 0, "ops")]),
            MemQ::unique(),
            MemQ::sort_by_key(|u: &User| u.score),
            MemQ::skip(1),
            MemQ::take(1),
        ],
        values,
    );

    assert_eq!(out.iter().map(|u| u.id).collect::<Vec<_>>(), vec![3]);
    assert_eq!(out[0].score, 11);
}

#[test]
fn memq_group_partition_and_fold_shape_results() {
    let values = arcs(vec![
        user(1, 30, 1, "sales"),
        user(2, 22, 2, "ops"),
        user(3, 34, 3, "sales"),
        user(4, 41, 4, "ops"),
    ]);

    let out = MemQ::apply_all(
        &[
            MemQ::group_by(|u: &User| u.team.clone()),
            MemQ::partition(|u: &User| u.team == "ops"),
            MemQ::fold(|values: Vec<Arc<User>>| values.into_iter().max_by_key(|u| u.age)),
        ],
        values,
    );

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].id, 4);
}

#[test]
fn punnu_scope_collect_returns_filtered_subset() {
    let punnu = Punnu::<User>::builder().build();
    block_on(async {
        punnu.insert(user(1, 30, 2, "sales")).await.unwrap();
        punnu.insert(user(2, 17, 3, "sales")).await.unwrap();
        punnu.insert(user(3, 40, 1, "ops")).await.unwrap();
    });

    let mut adult_ids = punnu
        .scope(Vec::new())
        .filter_basic(|f| f.age.gte(18))
        .collect()
        .into_iter()
        .map(|u| u.id)
        .collect::<Vec<_>>();
    adult_ids.sort_unstable();

    assert_eq!(adult_ids, vec![1, 3]);
}

#[test]
fn punnu_scope_iter_consumes_the_query_handle() {
    let punnu = Punnu::<User>::builder().build();
    block_on(async {
        punnu.insert(user(1, 30, 2, "sales")).await.unwrap();
        punnu.insert(user(2, 17, 3, "sales")).await.unwrap();
        punnu.insert(user(3, 40, 1, "ops")).await.unwrap();
    });

    let ids = punnu
        .scope(vec![MemQ::filter_basic(
            User::fields().team.eq("ops".to_owned()),
        )])
        .iter()
        .map(|u| u.id)
        .collect::<Vec<_>>();

    assert_eq!(ids, vec![3]);
}

#[test]
fn punnu_scope_filters_union_without_hidden_query_membership() {
    let punnu = Punnu::<User>::builder().build();
    block_on(async {
        punnu.insert(user(1, 30, 2, "sales")).await.unwrap();
        punnu.insert(user(2, 17, 3, "sales")).await.unwrap();
        punnu.insert(user(3, 40, 1, "ops")).await.unwrap();
        punnu.insert(user(4, 28, 8, "ops")).await.unwrap();
    });

    let mut adult_sales_ids = punnu
        .scope(Vec::new())
        .filter_basic(|f| f.team.eq("sales".to_owned()))
        .filter_basic(|f| f.age.gte(18))
        .collect()
        .into_iter()
        .map(|u| u.id)
        .collect::<Vec<_>>();
    adult_sales_ids.sort_unstable();

    let mut ops_ids = punnu
        .scope(Vec::new())
        .filter_basic(|f| f.team.eq("ops".to_owned()))
        .collect()
        .into_iter()
        .map(|u| u.id)
        .collect::<Vec<_>>();
    ops_ids.sort_unstable();

    assert_eq!(adult_sales_ids, vec![1]);
    assert_eq!(ops_ids, vec![3, 4]);
    assert_eq!(
        punnu.len(),
        4,
        "scopes must not encode hidden query membership in L1"
    );
}

#[test]
fn punnu_scope_chained_values_do_not_mutate_l1() {
    let punnu = Punnu::<User>::builder().build();
    block_on(async {
        punnu.insert(user(1, 30, 2, "sales")).await.unwrap();
    });

    let chained = punnu
        .scope(Vec::new())
        .chain_values(vec![user(99, 44, 9, "external")])
        .chain(vec![Arc::new(user(100, 45, 10, "external"))])
        .collect();
    let mut ids = chained.iter().map(|u| u.id).collect::<Vec<_>>();
    ids.sort_unstable();

    assert_eq!(ids, vec![1, 99, 100]);
    assert_eq!(punnu.len(), 1);
    assert!(
        punnu.get(&99).is_none(),
        "chain_values must not insert into L1"
    );
    assert!(punnu.get(&100).is_none(), "chain must not insert into L1");
}
