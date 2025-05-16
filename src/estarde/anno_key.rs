use graphannis::graph::AnnoKey;
use graphannis_core::util::{join_qname, split_qname};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::IntoInner;

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum MultiTypeAnnoKey {
    String(String),
    Key(AnnoKey),
}

impl IntoInner for MultiTypeAnnoKey {
    type I = AnnoKey;

    fn into_inner(self) -> Self::I {
        match self {
            MultiTypeAnnoKey::String(s) => {
                let (ns, name) = split_qname(&s);
                AnnoKey {
                    ns: ns.unwrap_or_default().into(),
                    name: name.into(),
                }
            }
            MultiTypeAnnoKey::Key(k) => k,
        }
    }
}

// offer a function that can deserialize an AnnoKey from String and from a map
pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<AnnoKey, D::Error> {
    let multi_key = MultiTypeAnnoKey::deserialize(deserializer)?;
    Ok(multi_key.into_inner())
}

pub(crate) fn serialize<S: Serializer>(value: &AnnoKey, serializer: S) -> Result<S::Ok, S::Error> {
    let serializable = join_qname(&value.ns, &value.name);
    serializable.serialize(serializer)
}

pub(crate) mod as_option {
    use graphannis::graph::AnnoKey;
    use graphannis_core::util::join_qname;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::estarde::IntoInner;

    use super::MultiTypeAnnoKey;

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<AnnoKey>, D::Error> {
        let multi_key = Option::<MultiTypeAnnoKey>::deserialize(deserializer)?;
        Ok(multi_key.map(|mk| mk.into_inner()))
    }

    pub fn serialize<S: Serializer>(
        value: &Option<AnnoKey>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if let Some(key) = value {
            let serializable = join_qname(&key.ns, &key.name);
            serializable.serialize(serializer)
        } else {
            serializer.serialize_none()
        }
    }
}

pub(crate) mod in_sequence {
    use std::iter::FromIterator;

    use graphannis::graph::AnnoKey;
    use graphannis_core::util::join_qname;
    use itertools::Itertools;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::estarde::IntoInner;

    use super::MultiTypeAnnoKey;

    pub fn deserialize<'de, D: Deserializer<'de>, T: FromIterator<AnnoKey>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        let multi_key_seq = Vec::<MultiTypeAnnoKey>::deserialize(deserializer)?;
        Ok(multi_key_seq
            .into_iter()
            .map(|mk| mk.into_inner())
            .collect::<T>())
    }

    pub fn serialize<'a, S: Serializer, T: IntoIterator<Item = &'a AnnoKey>>(
        value: T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let anno_component_vec = value
            .into_iter()
            .map(|k| join_qname(&k.ns, &k.name))
            .collect_vec();
        anno_component_vec.serialize(serializer)
    }
}
