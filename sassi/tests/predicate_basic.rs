//! Integration tests for [`sassi::BasicPredicate`] + the `Field<T, V>`
//! lookup-method surface.

use sassi::{BasicPredicate, Cacheable, Field};

#[derive(Debug, Clone)]
struct User {
    id: i64,
    name: String,
    age: u32,
    banned: bool,
    nickname: Option<String>,
}

#[derive(Default)]
struct UserFields {
    pub id: Field<User, i64>,
    pub name: Field<User, String>,
    pub age: Field<User, u32>,
    pub banned: Field<User, bool>,
    pub nickname: Field<User, Option<String>>,
}

impl Cacheable for User {
    type Id = i64;
    type Fields = UserFields;
    fn id(&self) -> i64 {
        self.id
    }
}

fn fields() -> UserFields {
    UserFields {
        id: Field::new("id", |u| &u.id),
        name: Field::new("name", |u| &u.name),
        age: Field::new("age", |u| &u.age),
        banned: Field::new("banned", |u| &u.banned),
        nickname: Field::new("nickname", |u| &u.nickname),
    }
}

fn alice() -> User {
    User {
        id: 1,
        name: "Alice".into(),
        age: 30,
        banned: false,
        nickname: Some("Al".into()),
    }
}

fn bob() -> User {
    User {
        id: 2,
        name: "BOB".into(),
        age: 17,
        banned: true,
        nickname: None,
    }
}

// === Equality / Inequality ===============================================

#[test]
fn eq_predicate_matches() {
    let f = fields();
    let p: BasicPredicate<User> = f.age.eq(30);
    assert!(p.evaluate(&alice()));
    assert!(!p.evaluate(&bob()));
}

#[test]
fn neq_predicate_matches() {
    let f = fields();
    let p: BasicPredicate<User> = f.age.neq(30);
    assert!(!p.evaluate(&alice()));
    assert!(p.evaluate(&bob()));
}

// === Ordering ============================================================

#[test]
fn gt_gte_lt_lte() {
    let f = fields();
    assert!(f.age.gt(20).evaluate(&alice()));
    assert!(!f.age.gt(20).evaluate(&bob()));
    assert!(f.age.gte(30).evaluate(&alice()));
    assert!(!f.age.gte(30).evaluate(&bob()));
    assert!(f.age.lt(20).evaluate(&bob()));
    assert!(f.age.lte(17).evaluate(&bob()));
}

#[test]
fn between_inclusive_both_ends() {
    let f = fields();
    assert!(f.age.between(17, 30).evaluate(&alice()));
    assert!(f.age.between(17, 30).evaluate(&bob()));
    assert!(!f.age.between(0, 16).evaluate(&bob()));
}

// === Membership ==========================================================

#[test]
fn in_and_not_in() {
    let f = fields();
    let in_pred: BasicPredicate<User> = f.age.in_(vec![17, 30, 99]);
    assert!(in_pred.evaluate(&alice()));
    assert!(in_pred.evaluate(&bob()));

    let not_in: BasicPredicate<User> = f.age.not_in(vec![17, 30]);
    assert!(!not_in.evaluate(&alice()));
    assert!(!not_in.evaluate(&bob()));
}

// === Null tests on Option<U> =============================================

#[test]
fn is_null_and_is_not_null() {
    let f = fields();
    assert!(f.nickname.is_null().evaluate(&bob()));
    assert!(!f.nickname.is_null().evaluate(&alice()));
    assert!(f.nickname.is_not_null().evaluate(&alice()));
    assert!(!f.nickname.is_not_null().evaluate(&bob()));
}

// === String operations ===================================================

#[test]
fn contains_case_sensitive() {
    let f = fields();
    let p: BasicPredicate<User> = f.name.contains("Ali");
    assert!(p.evaluate(&alice()));
    assert!(!p.evaluate(&bob()));
}

#[test]
fn icontains_uses_ascii_lowercase_no_regex() {
    let f = fields();
    let p: BasicPredicate<User> = f.name.icontains("bob");
    assert!(p.evaluate(&bob())); // BOB contains "bob" case-insensitively
    assert!(!p.evaluate(&alice()));
}

#[test]
fn starts_with_and_istarts_with() {
    let f = fields();
    assert!(f.name.starts_with("Al").evaluate(&alice()));
    assert!(!f.name.starts_with("al").evaluate(&alice()));
    assert!(f.name.istarts_with("al").evaluate(&alice()));
    assert!(f.name.istarts_with("BO").evaluate(&bob()));
}

#[test]
fn ends_with_and_iends_with() {
    let f = fields();
    assert!(f.name.ends_with("ce").evaluate(&alice()));
    assert!(!f.name.ends_with("CE").evaluate(&alice()));
    assert!(f.name.iends_with("ce").evaluate(&alice()));
    assert!(f.name.iends_with("OB").evaluate(&bob()));
}

#[test]
fn iexact_matches_case_insensitively() {
    let f = fields();
    assert!(f.name.iexact("alice").evaluate(&alice()));
    assert!(f.name.iexact("ALICE").evaluate(&alice()));
    assert!(f.name.iexact("BoB").evaluate(&bob()));
    assert!(!f.name.iexact("carol").evaluate(&alice()));
}

// === Algebra: AND / OR / XOR / NOT =======================================

#[test]
fn and_or_compose_and_flatten() {
    let f = fields();
    let adult: BasicPredicate<User> = f.age.gte(18);
    let banned: BasicPredicate<User> = f.banned.eq(true);

    let active_adult = adult.clone() & !banned.clone();
    assert!(active_adult.evaluate(&alice()));
    assert!(!active_adult.evaluate(&bob()));

    let either = adult.clone() | banned.clone();
    assert!(either.evaluate(&alice()));
    assert!(either.evaluate(&bob()));

    // AND flattening: a & b & c produces a single And node
    let three: BasicPredicate<User> = adult.clone() & f.age.lt(100) & f.id.gt(0);
    if let BasicPredicate::And(children) = three {
        assert_eq!(children.len(), 3, "And should flatten chained &");
    } else {
        panic!("expected And node");
    }
}

#[test]
fn xor_truth_table() {
    let f = fields();
    let adult: BasicPredicate<User> = f.age.gte(18);
    let banned: BasicPredicate<User> = f.banned.eq(true);
    let xor = adult ^ banned;

    // alice: adult=true, banned=false → XOR = true
    assert!(xor.evaluate(&alice()));
    // bob: adult=false (age 17), banned=true → XOR = true
    assert!(xor.evaluate(&bob()));
}

#[test]
fn xor_both_true_both_false() {
    let f = fields();
    let adult: BasicPredicate<User> = f.age.gte(18);
    let xor_self_true = adult.clone() ^ adult.clone();
    // alice: adult=true XOR adult=true = false
    assert!(!xor_self_true.evaluate(&alice()));
    // bob: adult=false XOR adult=false = false
    assert!(!xor_self_true.evaluate(&bob()));
}

#[test]
fn not_collapses_double_negation() {
    let f = fields();
    let p: BasicPredicate<User> = f.age.eq(30);
    let double = !!p.clone();
    // Should evaluate same as the original
    assert_eq!(double.evaluate(&alice()), p.evaluate(&alice()));
    assert_eq!(double.evaluate(&bob()), p.evaluate(&bob()));
    // And the AST should be the original Field node, not Not(Not(_))
    assert!(matches!(double, BasicPredicate::Field(_)));
}

#[test]
fn true_false_sentinels_evaluate() {
    let t: BasicPredicate<User> = BasicPredicate::True;
    let fa: BasicPredicate<User> = BasicPredicate::False;
    assert!(t.evaluate(&alice()));
    assert!(!fa.evaluate(&alice()));
    // !True == False, !False == True
    assert!(!(!t).evaluate(&alice()));
    assert!((!fa).evaluate(&alice()));
}
