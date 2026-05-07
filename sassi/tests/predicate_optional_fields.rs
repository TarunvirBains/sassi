//! Predicate regressions for optional-field and portable string semantics.

use sassi::{BasicPredicate, Cacheable, Field, IntoBasicPredicate, Punnu};

#[derive(Debug, Clone)]
struct Artifact {
    id: i64,
    title: String,
    estimated_year: Option<i32>,
}

#[derive(Default)]
struct ArtifactFields {
    #[allow(dead_code)]
    id: Field<Artifact, i64>,
    title: Field<Artifact, String>,
    estimated_year: Field<Artifact, Option<i32>>,
}

struct NonCloneArtifact {
    id: i64,
    estimated_year: Option<i32>,
}

#[derive(Default)]
struct NonCloneArtifactFields {
    estimated_year: Field<NonCloneArtifact, Option<i32>>,
}

impl Cacheable for NonCloneArtifact {
    type Id = i64;
    type Fields = NonCloneArtifactFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> NonCloneArtifactFields {
        NonCloneArtifactFields {
            estimated_year: Field::new("estimated_year", |artifact| &artifact.estimated_year),
        }
    }
}

impl Cacheable for Artifact {
    type Id = i64;
    type Fields = ArtifactFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> ArtifactFields {
        ArtifactFields {
            id: Field::new("id", |artifact| &artifact.id),
            title: Field::new("title", |artifact| &artifact.title),
            estimated_year: Field::new("estimated_year", |artifact| &artifact.estimated_year),
        }
    }
}

struct WrappedPredicate<T>(BasicPredicate<T>);

impl<T> IntoBasicPredicate<T> for WrappedPredicate<T> {
    fn into_basic_predicate(self) -> BasicPredicate<T> {
        self.0
    }
}

fn artifact(id: i64, title: &str, estimated_year: Option<i32>) -> Artifact {
    Artifact {
        id,
        title: title.to_owned(),
        estimated_year,
    }
}

#[test]
fn predicate_optional_fields_some_lte_matches_present_values_only() {
    let fields = Artifact::fields();
    let pred = fields.estimated_year.some().lte(2020);

    assert!(pred.evaluate(&artifact(1, "Alpha", Some(2019))));
    assert!(!pred.evaluate(&artifact(2, "Beta", Some(2021))));
    assert!(!pred.evaluate(&artifact(3, "Unknown", None)));
}

#[test]
fn predicate_optional_fields_basic_predicate_clones_without_model_clone() {
    let fields = NonCloneArtifact::fields();
    let pred: BasicPredicate<NonCloneArtifact> = fields.estimated_year.some().lte(2020);
    let cloned = pred.clone();

    let artifact = NonCloneArtifact {
        id: 1,
        estimated_year: Some(2019),
    };

    assert!(cloned.evaluate(&artifact));
}

#[test]
fn predicate_optional_fields_null_and_option_equality_semantics() {
    let fields = Artifact::fields();
    let missing = artifact(1, "Unknown", None);
    let found = artifact(2, "Alpha", Some(2020));

    assert!(fields.estimated_year.is_null().evaluate(&missing));
    assert!(!fields.estimated_year.is_null().evaluate(&found));
    assert!(fields.estimated_year.is_not_null().evaluate(&found));
    assert!(!fields.estimated_year.is_not_null().evaluate(&missing));

    assert!(fields.estimated_year.eq(None).evaluate(&missing));
    assert!(!fields.estimated_year.eq(None).evaluate(&found));
    assert!(fields.estimated_year.neq(Some(2020)).evaluate(&missing));
    assert!(!fields.estimated_year.neq(Some(2020)).evaluate(&found));
}

#[test]
fn predicate_optional_fields_some_membership_and_between_reject_none() {
    let fields = Artifact::fields();
    let missing = artifact(1, "Unknown", None);
    let old = artifact(2, "Old", Some(1999));
    let recent = artifact(3, "Recent", Some(2022));

    assert!(
        fields
            .estimated_year
            .some()
            .in_(vec![1999, 2020])
            .evaluate(&old)
    );
    assert!(
        !fields
            .estimated_year
            .some()
            .in_(vec![1999, 2020])
            .evaluate(&missing)
    );
    assert!(
        fields
            .estimated_year
            .some()
            .not_in(vec![1999, 2020])
            .evaluate(&recent)
    );
    assert!(
        !fields
            .estimated_year
            .some()
            .not_in(vec![1999, 2020])
            .evaluate(&missing)
    );
    assert!(
        fields
            .estimated_year
            .some()
            .between(1990, 2000)
            .evaluate(&old)
    );
    assert!(
        !fields
            .estimated_year
            .some()
            .between(1990, 2000)
            .evaluate(&missing)
    );
}

#[test]
fn predicate_optional_fields_string_iexact_is_ascii_stable() {
    let fields = Artifact::fields();
    let alpha = artifact(1, "Alpha%_\\Trail", Some(2020));
    let accented_upper = artifact(2, "Éclair", Some(2021));
    let accented_lower = artifact(3, "éclair", Some(2021));

    assert!(fields.title.iexact("alpha%_\\trail").evaluate(&alpha));
    assert!(fields.title.iexact("ALPHA%_\\TRAIL").evaluate(&alpha));
    assert!(!fields.title.iexact("éclair").evaluate(&accented_upper));
    assert!(!fields.title.iexact("Éclair").evaluate(&accented_lower));
    assert!(fields.title.iexact("Éclair").evaluate(&accented_upper));
    assert!(fields.title.iexact("éclair").evaluate(&accented_lower));
}

#[test]
fn predicate_optional_fields_string_icontains_is_ascii_stable() {
    let fields = Artifact::fields();
    let alpha = artifact(1, "Alpha%_\\Trail", Some(2020));
    let accented_upper = artifact(2, "Éclair", Some(2021));

    assert!(fields.title.icontains("alpha").evaluate(&alpha));
    assert!(fields.title.icontains("%_\\").evaluate(&alpha));
    assert!(fields.title.icontains("A%_\\T").evaluate(&alpha));
    assert!(!fields.title.icontains("écl").evaluate(&accented_upper));
    assert!(fields.title.icontains("Écl").evaluate(&accented_upper));
}

#[test]
fn predicate_optional_fields_string_istarts_with_is_ascii_stable() {
    let fields = Artifact::fields();
    let alpha = artifact(1, "Alpha%_\\Trail", Some(2020));
    let accented_upper = artifact(2, "Éclair", Some(2021));

    assert!(fields.title.istarts_with("alpha").evaluate(&alpha));
    assert!(fields.title.istarts_with("alpha%_\\").evaluate(&alpha));
    assert!(!fields.title.istarts_with("é").evaluate(&accented_upper));
    assert!(fields.title.istarts_with("É").evaluate(&accented_upper));
}

#[test]
fn predicate_optional_fields_string_iends_with_is_ascii_stable() {
    let fields = Artifact::fields();
    let alpha = artifact(1, "Alpha%_\\Trail", Some(2020));
    let accented_upper = artifact(2, "Café", Some(2021));

    assert!(fields.title.iends_with("\\trail").evaluate(&alpha));
    assert!(fields.title.iends_with("%_\\trail").evaluate(&alpha));
    assert!(!fields.title.iends_with("FÉ").evaluate(&accented_upper));
    assert!(fields.title.iends_with("fé").evaluate(&accented_upper));
}

#[tokio::test]
async fn predicate_optional_fields_punnu_accepts_into_basic_predicate() {
    let pool = Punnu::<Artifact>::builder().build();
    pool.insert(artifact(1, "Old", Some(1999))).await.unwrap();
    pool.insert(artifact(2, "Recent", Some(2022)))
        .await
        .unwrap();

    let results = pool
        .scope(Vec::new())
        .filter_basic(|fields| WrappedPredicate(fields.estimated_year.some().lte(2020)))
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, 1);
}
