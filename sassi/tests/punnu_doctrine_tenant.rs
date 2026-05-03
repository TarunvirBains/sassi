use sassi::{Cacheable, Punnu, PunnuConfig};

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserById {
    id: i64,
    tenant: &'static str,
}

impl Cacheable for UserById {
    type Id = i64;
    type Fields = ();

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {}
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct TenantUserId {
    tenant: &'static str,
    id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserByTenantId {
    key: TenantUserId,
}

impl Cacheable for UserByTenantId {
    type Id = TenantUserId;
    type Fields = ();

    fn id(&self) -> Self::Id {
        self.key.clone()
    }

    fn fields() -> Self::Fields {}
}

#[tokio::test]
async fn namespace_does_not_isolate_l1_identity() {
    let users = Punnu::<UserById>::builder()
        .config(PunnuConfig {
            namespace: Some("tenant-a".to_owned()),
            ..Default::default()
        })
        .build();

    users.insert(UserById { id: 1, tenant: "a" }).await.unwrap();
    users.insert(UserById { id: 1, tenant: "b" }).await.unwrap();

    assert_eq!(users.get(&1).unwrap().tenant, "b");
}

#[tokio::test]
async fn tenant_identity_belongs_in_the_cache_key_when_values_share_a_pool() {
    let users = Punnu::<UserByTenantId>::builder().build();
    let tenant_a = TenantUserId { tenant: "a", id: 1 };
    let tenant_b = TenantUserId { tenant: "b", id: 1 };

    users
        .insert(UserByTenantId {
            key: tenant_a.clone(),
        })
        .await
        .unwrap();
    users
        .insert(UserByTenantId {
            key: tenant_b.clone(),
        })
        .await
        .unwrap();

    assert_eq!(users.get(&tenant_a).unwrap().key.tenant, "a");
    assert_eq!(users.get(&tenant_b).unwrap().key.tenant, "b");
}
