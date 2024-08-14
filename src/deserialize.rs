// This module provides intermediate structs for types that cannot be deserialized easily.

use std::iter::FromIterator;

use graphannis::{
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::util::split_qname;
use serde::{Deserialize, Deserializer};

pub trait IntoInner {
    type I;
    fn into_inner(self) -> Self::I;
}

/// graphannis' annotation components cannot be deserialized from toml as they use the C representation.
/// Fortunately, their parts can.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeserializableComponent {
    ctype: AnnotationComponentType,
    layer: String,
    name: String,
}

impl IntoInner for DeserializableComponent {
    type I = AnnotationComponent;

    fn into_inner(self) -> Self::I {
        AnnotationComponent::new(self.ctype, self.layer.into(), self.name.into())
    }
}

pub fn deserialize_annotation_component<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<AnnotationComponent, D::Error> {
    let dc = DeserializableComponent::deserialize(deserializer)?;
    Ok(dc.into_inner())
}

pub fn deserialize_annotation_component_opt<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<AnnotationComponent>, D::Error> {
    let dc_opt = Option::<DeserializableComponent>::deserialize(deserializer)?;
    Ok(dc_opt.map(|d| d.into_inner()))
}

pub fn deserialize_annotation_component_seq<
    'de,
    D: Deserializer<'de>,
    T: FromIterator<AnnotationComponent>,
>(
    deserializer: D,
) -> Result<T, D::Error> {
    let component_seq = Vec::<DeserializableComponent>::deserialize(deserializer)?;
    Ok(component_seq
        .into_iter()
        .map(|dc| dc.into_inner())
        .collect::<T>())
}

// offer a function that can deserialize an AnnoKey from String and from a map
pub fn deserialize_anno_key<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<AnnoKey, D::Error> {
    let multi_key = MultiTypeAnnoKey::deserialize(deserializer)?;
    Ok(multi_key.into_inner())
}

pub fn deserialize_anno_key_opt<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<AnnoKey>, D::Error> {
    let multi_key = Option::<MultiTypeAnnoKey>::deserialize(deserializer)?;
    Ok(multi_key.map(|mk| mk.into_inner()))
}

pub fn deserialize_anno_key_seq<'de, D: Deserializer<'de>, T: FromIterator<AnnoKey>>(
    deserializer: D,
) -> Result<T, D::Error> {
    let multi_key_seq = Vec::<MultiTypeAnnoKey>::deserialize(deserializer)?;
    Ok(multi_key_seq
        .into_iter()
        .map(|mk| mk.into_inner())
        .collect::<T>())
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum MultiTypeAnnoKey {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use graphannis::{graph::AnnoKey, model::AnnotationComponentType};
    use graphannis_core::graph::ANNIS_NS;
    use serde::Deserialize;

    use crate::deserialize::IntoInner;

    use super::DeserializableComponent;
    use super::{deserialize_anno_key, deserialize_anno_key_opt, deserialize_anno_key_seq};

    #[test]
    fn component() {
        let toml_str = r#"
ctype = "Pointing"
layer = "syntax"
name = "dependency"
"#;
        let r: Result<DeserializableComponent, _> = toml::from_str(toml_str);
        assert!(r.is_ok());
        let dc = r.unwrap();
        let c = dc.into_inner();
        assert!(matches!(c.get_type(), AnnotationComponentType::Pointing));
        assert_eq!(c.layer.as_str(), "syntax");
        assert_eq!(c.name.as_str(), "dependency");
    }

    #[derive(Deserialize)]
    struct KeyOwner {
        #[serde(deserialize_with = "deserialize_anno_key")]
        key: AnnoKey,
        #[serde(deserialize_with = "deserialize_anno_key_opt")]
        optional: Option<AnnoKey>,
        #[serde(deserialize_with = "deserialize_anno_key_seq")]
        seq: Vec<AnnoKey>,
        #[serde(deserialize_with = "deserialize_anno_key_seq")]
        unique_seq: BTreeSet<AnnoKey>,
    }

    #[test]
    fn anno_key() {
        let with_strings = r#"
key = "annis::tok"
optional = "norm::norm"
seq = ["dipl", "dipl::cu", "norm::lemma"]
unique_seq = ["norm::pos", "norm::pos_lang"]
"#;
        let with_keys = r#"
key = { ns = "annis", name = "tok"}
optional = { ns = "norm", name = "norm" }
seq = [{ ns = "", name = "dipl" }, { ns = "dipl", name = "cu" }, { ns = "norm", name = "lemma" }]
unique_seq = [ { ns = "norm", name = "pos" }, { ns = "norm", name = "pos_lang" }]
"#;
        let frm_str: Result<KeyOwner, _> = toml::from_str(with_strings);
        assert!(
            frm_str.is_ok(),
            "Error deserializing from string keys: {:?}",
            frm_str.err()
        );
        let from_str = frm_str.unwrap();
        let frm_ks: Result<KeyOwner, _> = toml::from_str(with_keys);
        assert!(
            frm_ks.is_ok(),
            "Error deserializing from keys: {:?}",
            frm_ks.err()
        );
        let from_keys = frm_ks.unwrap();
        let tok = AnnoKey {
            ns: ANNIS_NS.into(),
            name: "tok".into(),
        };
        assert_eq!(from_str.key, from_keys.key);
        assert_eq!(from_str.key, tok);
        let norm_key = AnnoKey {
            ns: "norm".into(),
            name: "norm".into(),
        };
        assert!(from_str.optional.is_some());
        assert!(from_keys.optional.is_some());
        assert_eq!(&norm_key, from_str.optional.as_ref().unwrap());
        assert_eq!(&norm_key, from_keys.optional.as_ref().unwrap());
        let seq_keys = vec![
            AnnoKey {
                ns: "".into(),
                name: "dipl".into(),
            },
            AnnoKey {
                ns: "dipl".into(),
                name: "cu".into(),
            },
            AnnoKey {
                ns: "norm".into(),
                name: "lemma".into(),
            },
        ];
        assert_eq!(seq_keys.len(), from_str.seq.len());
        assert_eq!(from_keys.seq.len(), from_str.seq.len());
        for (k_exp, (k1, k2)) in seq_keys
            .iter()
            .zip(from_str.seq.iter().zip(from_keys.seq.iter()))
        {
            assert_eq!(k_exp, k1);
            assert_eq!(k1, k2);
        }
        let unique_keys: BTreeSet<AnnoKey> = vec![
            AnnoKey {
                ns: "norm".into(),
                name: "pos".into(),
            },
            AnnoKey {
                ns: "norm".into(),
                name: "pos_lang".into(),
            },
        ]
        .into_iter()
        .collect();
        assert_eq!(unique_keys, from_str.unique_seq);
        assert_eq!(from_keys.unique_seq, from_str.unique_seq);
    }
}
