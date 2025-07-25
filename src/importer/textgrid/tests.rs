use crate::test_util::import_as_graphml_string;

use super::*;
use insta::assert_snapshot;

#[test]
fn serialize() {
    let module = ImportTextgrid::default();
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

#[test]
fn serialize_custom() {
    let module = ImportTextgrid {
        audio_extension: "mp3".to_string(),
        tier_groups: Some(
            vec![(
                "tok".to_string(),
                vec!["pos".to_string(), "stress".to_string()]
                    .into_iter()
                    .collect(),
            )]
            .into_iter()
            .collect(),
        ),
        skip_timeline_generation: true,
        skip_audio: false,
        skip_time_annotations: true,
    };
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

#[test]
fn single_speaker() {
    let mut tg = BTreeMap::new();
    tg.insert(
        "tok".to_string(),
        vec![
            "lemma".to_string(),
            "pos".to_string(),
            "Inf-Struct".to_string(),
        ]
        .into_iter()
        .collect(),
    );
    let actual = import_as_graphml_string(
        ImportTextgrid {
            tier_groups: Some(tg),
            skip_timeline_generation: true,
            skip_audio: false,
            skip_time_annotations: false,
            ..Default::default()
        },
        Path::new("tests/data/import/textgrid/singleSpeaker"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn empty_intervals() {
    let mut tg = BTreeMap::new();
    tg.insert(
        "tok".to_string(),
        vec!["lemma".to_string(), "pos".to_string()]
            .into_iter()
            .collect(),
    );
    let actual = import_as_graphml_string(
        ImportTextgrid {
            tier_groups: Some(tg),
            skip_timeline_generation: true,
            skip_audio: false,
            skip_time_annotations: false,
            ..Default::default()
        },
        Path::new("tests/data/import/textgrid/emptyIntervals"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn two_speakers() {
    let mut tg = BTreeMap::new();
    tg.insert(
        "A".to_string(),
        vec![
            "lemma".to_string(),
            "pos".to_string(),
            "Inf-Struct".to_string(),
        ]
        .into_iter()
        .collect(),
    );
    tg.insert("B".to_string(), BTreeSet::new());
    let actual = import_as_graphml_string(
        ImportTextgrid {
            tier_groups: Some(tg),
            skip_timeline_generation: false,
            skip_audio: false,
            skip_time_annotations: false,
            ..Default::default()
        },
        Path::new("tests/data/import/textgrid/twoSpeakers"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn misaligned_lemma_annotation() {
    let mut tg = BTreeMap::new();
    tg.insert(
        "A".to_string(),
        vec![
            "lemma".to_string(),
            "pos".to_string(),
            "Inf-Struct".to_string(),
        ]
        .into_iter()
        .collect(),
    );
    tg.insert("B".to_string(), BTreeSet::new());
    let actual = import_as_graphml_string(
        ImportTextgrid {
            tier_groups: Some(tg),
            skip_timeline_generation: false,
            skip_audio: false,
            skip_time_annotations: false,
            ..Default::default()
        },
        Path::new("tests/data/import/textgrid/misalignedLemma"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn fail_wrong_map() {
    let mut tg = BTreeMap::new();
    tg.insert("B".to_string(), vec!["A".to_string()].into_iter().collect());
    assert!(
        ImportTextgrid {
            tier_groups: Some(tg),
            skip_timeline_generation: false,
            skip_audio: false,
            skip_time_annotations: false,
            ..Default::default()
        }
        .import_corpus(
            Path::new("tests/data/import/textgrid/fail_wrong_map"),
            StepID {
                module_name: "test_failing_import".to_string(),
                path: None
            },
            None
        )
        .is_err()
    );
}
