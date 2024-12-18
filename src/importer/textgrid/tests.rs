use crate::test_util::import_as_graphml_string;

use super::*;
use insta::assert_snapshot;

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
            audio_extension: None,
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
            audio_extension: None,
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
            audio_extension: None,
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
            audio_extension: None,
        },
        Path::new("tests/data/import/textgrid/misalignedLemma"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}
