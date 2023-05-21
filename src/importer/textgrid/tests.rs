use crate::util::import_as_graphml_string;

use super::*;
use insta::assert_snapshot;
use pretty_assertions::assert_eq;

#[test]
fn parse_tier_groups_param() {
    let result = parse_tier_map("A={lemma,pos,Inf-Struct};B={}");
    assert_eq!(2, result.len());
    let a = result.get("A").unwrap();
    assert_eq!(3, a.len());
    assert!(a.contains("lemma"));
    assert!(a.contains("pos"));
    assert!(a.contains("Inf-Struct"));
    let b = result.get("B").unwrap();
    assert_eq!(0, b.len());
}

#[test]
fn single_speaker() {
    let actual = import_as_graphml_string(
        TextgridImporter {
            tier_groups: Some("tok={lemma,pos,Inf-Struct}".to_string()),
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
fn two_speakers() {
    let actual = import_as_graphml_string(
        TextgridImporter {
            tier_groups: Some("A={lemma,pos,Inf-Struct};B={}".to_string()),
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
    let actual = import_as_graphml_string(
        TextgridImporter {
            tier_groups: Some("A={lemma,pos,Inf-Struct};B={}".to_string()),
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
