use sassi::{Cacheable, Punnu};

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserCanonical {
    id: i64,
    email: &'static str,
}

impl Cacheable for UserCanonical {
    type Id = i64;
    type Fields = ();

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserVisage {
    id: i64,
    display_name: &'static str,
}

impl Cacheable for UserVisage {
    type Id = i64;
    type Fields = ();

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {}
}

#[tokio::test]
async fn canonical_and_visage_punnus_are_type_disjoint() {
    let canonical = Punnu::<UserCanonical>::builder().build();
    let visage = Punnu::<UserVisage>::builder().build();

    canonical
        .insert(UserCanonical {
            id: 1,
            email: "ada@example.com",
        })
        .await
        .unwrap();
    visage
        .insert(UserVisage {
            id: 1,
            display_name: "Ada",
        })
        .await
        .unwrap();

    assert_eq!(canonical.get(&1).unwrap().email, "ada@example.com");
    assert_eq!(visage.get(&1).unwrap().display_name, "Ada");
}
