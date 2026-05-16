use sassi::{
    BasicPredicate, Cacheable, Field, JCompareOp, JFiniteF64, JObject, JSahibON,
    JSahibONPredicateBody, JTypeKind, LookupOp, evaluate_jsahibon_predicate,
};

#[derive(Debug, Clone)]
struct Document {
    id: i64,
    payload: JSahibON,
    maybe_payload: Option<JSahibON>,
}

#[derive(Default)]
struct DocumentFields {
    pub id: Field<Document, i64>,
    pub payload: Field<Document, JSahibON>,
    pub maybe_payload: Field<Document, Option<JSahibON>>,
}

impl Cacheable for Document {
    type Id = i64;
    type Fields = DocumentFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> DocumentFields {
        DocumentFields {
            id: Field::new("id", |document| &document.id),
            payload: Field::new("payload", |document| &document.payload),
            maybe_payload: Field::new("maybe_payload", |document| &document.maybe_payload),
        }
    }
}

fn object(entries: impl IntoIterator<Item = (&'static str, JSahibON)>) -> JSahibON {
    JSahibON::Object(JObject::from_entries(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value)),
    ))
}

fn document(payload: JSahibON, maybe_payload: Option<JSahibON>) -> Document {
    Document {
        id: 1,
        payload,
        maybe_payload,
    }
}

#[test]
fn postcard_roundtrips_all_value_variants_and_preserves_object_order() {
    let value = object([
        ("null", JSahibON::Null),
        ("bool", JSahibON::Bool(true)),
        ("i64", JSahibON::I64(-7)),
        ("u64", JSahibON::U64(u64::MAX)),
        ("f64", JSahibON::try_f64(1.5).unwrap()),
        ("string", JSahibON::String("hello".to_owned())),
        (
            "array",
            JSahibON::Array(vec![JSahibON::Null, JSahibON::String("x".to_owned())]),
        ),
    ]);

    let bytes = postcard::to_allocvec(&value).unwrap();
    let decoded: JSahibON = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(decoded, value);
    let JSahibON::Object(decoded_object) = decoded else {
        panic!("expected object");
    };
    assert_eq!(
        decoded_object
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["null", "bool", "i64", "u64", "f64", "string", "array"]
    );
}

#[test]
fn postcard_variant_order_is_wire_pinned() {
    assert_eq!(postcard::to_allocvec(&JSahibON::Null).unwrap()[0], 0);
    assert_eq!(
        postcard::to_allocvec(&JSahibON::Object(JObject::new())).unwrap()[0],
        7
    );
}

#[test]
fn finite_float_rejects_non_finite_constructors_and_deserialization() {
    assert!(JFiniteF64::try_new(f64::NAN).is_err());
    assert!(JFiniteF64::try_new(f64::INFINITY).is_err());
    assert!(JFiniteF64::try_new(f64::NEG_INFINITY).is_err());
    assert!(JSahibON::try_f64(f64::NAN).is_err());

    let bytes = postcard::to_allocvec(&f64::INFINITY).unwrap();
    let decoded = postcard::from_bytes::<JFiniteF64>(&bytes);
    assert!(decoded.is_err());
}

#[test]
fn object_equality_is_order_insensitive_but_iteration_order_is_stable() {
    let first = JObject::from_entries([
        ("a".to_owned(), JSahibON::I64(1)),
        ("b".to_owned(), JSahibON::Bool(true)),
    ]);
    let second = JObject::from_entries([
        ("b".to_owned(), JSahibON::Bool(true)),
        ("a".to_owned(), JSahibON::U64(1)),
    ]);

    assert_eq!(first, second);
    assert_eq!(
        first
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(
        second
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["b", "a"]
    );
}

#[test]
fn duplicate_object_keys_replace_value_without_moving_position() {
    let object = JObject::from_entries([
        ("a".to_owned(), JSahibON::I64(1)),
        ("b".to_owned(), JSahibON::I64(2)),
        ("a".to_owned(), JSahibON::I64(3)),
    ]);

    assert_eq!(
        object
            .iter()
            .map(|(key, value)| (key.as_str(), value))
            .collect::<Vec<_>>(),
        vec![("a", &JSahibON::I64(3)), ("b", &JSahibON::I64(2))]
    );
}

#[test]
fn numeric_equality_softens_across_number_carriers() {
    assert_eq!(JSahibON::I64(1), JSahibON::U64(1));
    assert_eq!(JSahibON::I64(1), JSahibON::try_f64(1.0).unwrap());
    assert_eq!(
        JSahibON::try_f64(0.0).unwrap(),
        JSahibON::try_f64(-0.0).unwrap()
    );
    assert_ne!(
        JSahibON::U64(u64::MAX),
        JSahibON::try_f64(u64::MAX as f64).unwrap()
    );
}

#[test]
fn path_key_and_array_predicates_use_portable_json_semantics() {
    let fields = Document::fields();
    assert_eq!(fields.id.name(), "id");
    let payload = object([
        (
            "profile",
            object([
                ("age", JSahibON::U64(30)),
                ("name", JSahibON::String("Ada".to_owned())),
                (
                    "content-type",
                    JSahibON::String("application/json".to_owned()),
                ),
                ("a.b", JSahibON::Bool(true)),
                ("0", JSahibON::String("zero-key".to_owned())),
                ("", JSahibON::String("empty-key".to_owned())),
                ("cafe", JSahibON::String("ascii".to_owned())),
                ("cafe\u{301}", JSahibON::String("unicode".to_owned())),
            ]),
        ),
        (
            "scores",
            JSahibON::Array(vec![
                JSahibON::I64(1),
                JSahibON::try_f64(2.0).unwrap(),
                JSahibON::String("x".to_owned()),
            ]),
        ),
    ]);
    let document = document(payload, None);

    assert!(
        fields
            .payload
            .jsahibon()
            .path("profile.age")
            .value::<u64>()
            .gte(30)
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .path("profile")
            .key("content-type")
            .value::<String>()
            .eq("application/json".to_owned())
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .path("profile")
            .key("a.b")
            .value::<bool>()
            .eq(true)
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .path("profile")
            .key("0")
            .value::<String>()
            .eq("zero-key".to_owned())
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .path("profile")
            .key("")
            .exists()
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .path_segments(["profile", "cafe\u{301}"])
            .value::<String>()
            .eq("unicode".to_owned())
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("scores")
            .array_contains(JSahibON::U64(1))
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("scores")
            .array_len_eq(3)
            .evaluate(&document)
    );
}

#[test]
fn missing_null_type_mismatch_and_option_none_have_distinct_semantics() {
    let fields = Document::fields();
    let payload = object([
        ("present_null", JSahibON::Null),
        ("number", JSahibON::I64(5)),
        ("array", JSahibON::Array(vec![JSahibON::Bool(true)])),
    ]);
    let none_doc = document(payload.clone(), None);
    let null_doc = document(payload, Some(JSahibON::Null));

    assert!(
        fields
            .payload
            .jsahibon()
            .key("present_null")
            .exists()
            .evaluate(&none_doc)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("present_null")
            .is_json_null()
            .evaluate(&none_doc)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("present_null")
            .is_json_null()
            .evaluate(&none_doc)
    );
    assert!(
        !fields
            .payload
            .jsahibon()
            .key("missing")
            .is_json_null()
            .evaluate(&none_doc)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("missing")
            .missing()
            .evaluate(&none_doc)
    );
    assert!(
        !fields
            .payload
            .jsahibon()
            .key("number")
            .has_key("x")
            .evaluate(&none_doc)
    );
    assert!(
        !fields
            .payload
            .jsahibon()
            .key("number")
            .value::<String>()
            .eq("5".to_owned())
            .evaluate(&none_doc)
    );
    assert!(
        !fields
            .payload
            .jsahibon()
            .key("missing")
            .value::<i64>()
            .not_in(vec![])
            .evaluate(&none_doc)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("number")
            .value::<i64>()
            .not_in(vec![])
            .evaluate(&none_doc)
    );
    assert!(
        !fields
            .payload
            .jsahibon()
            .key("number")
            .eq_json(JSahibON::U64(99))
            .evaluate(&none_doc)
    );
    assert!(
        !fields
            .payload
            .jsahibon()
            .key("missing")
            .neq_json(JSahibON::Null)
            .evaluate(&none_doc)
    );

    assert!(
        fields
            .maybe_payload
            .jsahibon()
            .missing()
            .evaluate(&none_doc)
    );
    assert!(!fields.maybe_payload.jsahibon().exists().evaluate(&none_doc));
    assert!(fields.maybe_payload.jsahibon().exists().evaluate(&null_doc));
    assert!(
        fields
            .maybe_payload
            .jsahibon()
            .is_json_null()
            .evaluate(&null_doc)
    );
    assert!(
        fields
            .maybe_payload
            .jsahibon()
            .is_json_null()
            .evaluate(&null_doc)
    );
}

#[test]
fn type_predicates_match_only_existing_json_values() {
    let fields = Document::fields();
    let payload = object([
        ("null", JSahibON::Null),
        ("bool", JSahibON::Bool(true)),
        ("number", JSahibON::I64(5)),
        ("string", JSahibON::String("value".to_owned())),
        ("array", JSahibON::Array(vec![])),
        ("object", object([])),
    ]);
    let document = document(payload, None);

    assert!(
        fields
            .payload
            .jsahibon()
            .key("null")
            .is_type(JTypeKind::Null)
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("bool")
            .is_bool()
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("number")
            .is_number()
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("string")
            .is_string()
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("array")
            .is_array()
            .evaluate(&document)
    );
    assert!(
        fields
            .payload
            .jsahibon()
            .key("object")
            .is_object()
            .evaluate(&document)
    );
    assert!(
        !fields
            .payload
            .jsahibon()
            .key("missing")
            .is_type(JTypeKind::Null)
            .evaluate(&document)
    );
    assert!(
        !fields
            .payload
            .jsahibon()
            .key("number")
            .is_string()
            .evaluate(&document)
    );
}

#[test]
fn predicate_payload_is_inspectable_under_lookup_op_json() {
    let fields = Document::fields();
    let predicate: BasicPredicate<Document> = fields
        .payload
        .jsahibon()
        .path("profile.age")
        .value::<u64>()
        .gte(21);

    let BasicPredicate::Field(field) = predicate else {
        panic!("expected field predicate");
    };
    assert_eq!(field.op(), LookupOp::Json);
    let body = field.value_as::<JSahibONPredicateBody>().unwrap();
    let JSahibONPredicateBody::ScalarCompare {
        path,
        op,
        scalar_kind: _,
        operand: _,
    } = body
    else {
        panic!("expected scalar compare body");
    };
    assert_eq!(path.segments(), ["profile", "age"]);
    assert_eq!(*op, JCompareOp::Gte);
}

#[test]
fn pure_evaluator_matches_predicate_truth_rules() {
    let root = object([(
        "array",
        JSahibON::Array(vec![JSahibON::try_f64(1.0).unwrap()]),
    )]);
    let body = JSahibONPredicateBody::ArrayContains {
        path: ["array"].into_iter().collect(),
        element: JSahibON::I64(1),
    };

    assert!(evaluate_jsahibon_predicate(Some(&root), &body));
    assert!(!evaluate_jsahibon_predicate(None, &body));
}

#[test]
#[should_panic(expected = "JSahibON scalar predicates only accept finite f64 operands")]
fn f64_scalar_predicate_construction_rejects_nan_operand() {
    // Spec: "Non-finite operands are rejected at predicate construction."
    // Construction here = the `.eq(...)` call below; the panic surfaces from
    // `JScalar for f64::into_scalar_value` so the fluent predicate API can
    // remain infallible-typed (`-> BasicPredicate<T>`) without rippling a
    // `Result` through every comparison method.
    let _ = Document::fields()
        .payload
        .jsahibon()
        .key("score")
        .value::<f64>()
        .eq(f64::NAN);
}

#[test]
#[should_panic(expected = "JSahibON scalar predicates only accept finite f64 operands")]
fn f64_scalar_predicate_construction_rejects_infinity_operand() {
    let _ = Document::fields()
        .payload
        .jsahibon()
        .key("score")
        .value::<f64>()
        .gte(f64::INFINITY);
}

#[test]
#[should_panic(expected = "JSahibON scalar predicates only accept finite f64 operands")]
fn f64_scalar_between_rejects_non_finite_high_bound() {
    let _ = Document::fields()
        .payload
        .jsahibon()
        .key("score")
        .value::<f64>()
        .between(0.0, f64::NEG_INFINITY);
}

#[test]
#[should_panic(expected = "JSahibON scalar predicates only accept finite f64 operands")]
fn f64_scalar_between_rejects_non_finite_low_bound() {
    let _ = Document::fields()
        .payload
        .jsahibon()
        .key("score")
        .value::<f64>()
        .between(f64::NAN, 1.0);
}
