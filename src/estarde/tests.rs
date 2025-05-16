use std::collections::BTreeSet;

use graphannis::{graph::AnnoKey, model::AnnotationComponentType};
use graphannis_core::graph::ANNIS_NS;
use serde::Deserialize;

use crate::estarde::IntoInner;

use super::annotation_component::SerdeComponent;

#[test]
fn component() {
    let toml_str = r#"
ctype = "Pointing"
layer = "syntax"
name = "dependency"
"#;
    let r: Result<SerdeComponent, _> = toml::from_str(toml_str);
    assert!(r.is_ok());
    let dc = r.unwrap();
    let c = dc.into_inner();
    assert!(matches!(c.get_type(), AnnotationComponentType::Pointing));
    assert_eq!(c.layer.as_str(), "syntax");
    assert_eq!(c.name.as_str(), "dependency");
}

#[derive(Deserialize)]
struct KeyOwner {
    #[serde(with = "crate::estarde::anno_key")]
    key: AnnoKey,
    #[serde(with = "crate::estarde::anno_key::as_option")]
    optional: Option<AnnoKey>,
    #[serde(with = "crate::estarde::anno_key::in_sequence")]
    seq: Vec<AnnoKey>,
    #[serde(with = "crate::estarde::anno_key::in_sequence")]
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
