use sassi::{Cacheable, Punnu};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct CollectionKey {
    tenant: &'static str,
    filter: &'static str,
    page: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserCollection {
    key: CollectionKey,
    user_ids: Vec<i64>,
}

impl Cacheable for UserCollection {
    type Id = CollectionKey;
    type Fields = ();

    fn id(&self) -> Self::Id {
        self.key.clone()
    }

    fn fields() -> Self::Fields {}
}

#[tokio::test]
async fn query_specific_collection_wrappers_encode_lookup_dimensions() {
    let collections = Punnu::<UserCollection>::builder().build();
    let active_page_one = CollectionKey {
        tenant: "acme",
        filter: "active",
        page: 1,
    };
    let active_page_two = CollectionKey {
        tenant: "acme",
        filter: "active",
        page: 2,
    };

    collections
        .insert(UserCollection {
            key: active_page_one.clone(),
            user_ids: Vec::new(),
        })
        .await
        .unwrap();

    assert_eq!(
        collections.get(&active_page_one).unwrap().user_ids,
        Vec::<i64>::new()
    );
    assert!(collections.get(&active_page_two).is_none());
}
