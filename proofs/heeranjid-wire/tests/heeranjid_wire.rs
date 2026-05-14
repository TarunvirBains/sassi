use heeranjid::{HeerId, HeerIdDesc, RanjId, RanjIdDesc, RanjPrecision};
use sassi::{Cacheable, Punnu, wire};
use serde::{Deserialize, Serialize};

#[derive(Cacheable, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cacheable(type_name = "sassi.proof.HeerModel")]
struct HeerModel {
    id: HeerId,
    label: String,
}

#[derive(Cacheable, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cacheable(type_name = "sassi.proof.RanjModel")]
struct RanjModel {
    id: RanjId,
    label: String,
}

#[derive(Cacheable, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cacheable(type_name = "sassi.proof.HeerDescModel")]
struct HeerDescModel {
    id: HeerIdDesc,
    label: String,
}

#[derive(Cacheable, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cacheable(type_name = "sassi.proof.RanjDescModel")]
struct RanjDescModel {
    id: RanjIdDesc,
    label: String,
}

fn assert_wire_roundtrip<T>(value: T)
where
    T: Cacheable + Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = wire::to_vec(&value).unwrap();
    let decoded = wire::from_slice::<T>(&bytes).unwrap();

    assert_eq!(decoded, value);
}

async fn assert_punnu_lookup<T>(value: T)
where
    T: Cacheable + Clone + PartialEq + std::fmt::Debug,
{
    let id = value.id();
    let punnu = Punnu::<T>::builder().build();

    punnu.insert(value.clone()).await.unwrap();
    let cached = punnu.get(&id).unwrap();

    assert_eq!(cached.as_ref(), &value);
}

#[test]
fn sassi_wire_round_trips_cacheable_models_with_heeranjid_ids() {
    assert_wire_roundtrip(HeerModel {
        id: HeerId::new(55, 7, 9).unwrap(),
        label: "heer".into(),
    });
    assert_wire_roundtrip(RanjModel {
        id: RanjId::new(55, RanjPrecision::Microseconds, 7, 9).unwrap(),
        label: "ranj".into(),
    });
    assert_wire_roundtrip(HeerDescModel {
        id: HeerIdDesc::new(55, 7, 9).unwrap(),
        label: "heer-desc".into(),
    });
    assert_wire_roundtrip(RanjDescModel {
        id: RanjIdDesc::new(55, RanjPrecision::Microseconds, 7, 9).unwrap(),
        label: "ranj-desc".into(),
    });
}

#[tokio::test]
async fn punnu_looks_up_cacheable_models_by_heeranjid_ids() {
    assert_punnu_lookup(HeerModel {
        id: HeerId::new(55, 7, 9).unwrap(),
        label: "heer".into(),
    })
    .await;
    assert_punnu_lookup(RanjModel {
        id: RanjId::new(55, RanjPrecision::Microseconds, 7, 9).unwrap(),
        label: "ranj".into(),
    })
    .await;
    assert_punnu_lookup(HeerDescModel {
        id: HeerIdDesc::new(55, 7, 9).unwrap(),
        label: "heer-desc".into(),
    })
    .await;
    assert_punnu_lookup(RanjDescModel {
        id: RanjIdDesc::new(55, RanjPrecision::Microseconds, 7, 9).unwrap(),
        label: "ranj-desc".into(),
    })
    .await;
}
