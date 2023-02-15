use std::io::BufWriter;

use crate::util::import_as_graphml_string;

use super::*;
use graphannis::AnnotationGraph;
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
    let mut properties: BTreeMap<String, String> = BTreeMap::new();
    properties.insert(
        "tier_groups".to_string(),
        "tok={lemma,pos,Inf-Struct}".to_string(),
    );
    properties.insert("skip_timeline_generation".to_string(), "true".to_string());

    let actual = import_as_graphml_string::<TextgridImporter, _>(
        Path::new("tests/data/import/textgrid/input/singleSpeaker"),
        properties,
    )
    .unwrap();

    // Compare the actual output with the expected one
    assert_eq!(
        include_str!("../../../tests/data/import/textgrid/expected/singleSpeaker.graphml"),
        actual
    );
}

#[test]
fn two_speakers() {
    let mut properties: BTreeMap<String, String> = BTreeMap::new();
    properties.insert(
        "tier_groups".to_string(),
        "A={lemma,pos,Inf-Struct};B={}".to_string(),
    );
    let actual = import_as_graphml_string::<TextgridImporter, _>(
        Path::new("tests/data/import/textgrid/input/twoSpeakers"),
        properties,
    )
    .unwrap();

    // Compare the actual output with the expected one
    assert_eq!(
        include_str!("../../../tests/data/import/textgrid/expected/twoSpeakers.graphml"),
        actual
    );
}

#[test]
fn misaligned_lemma_annotation() {
    let mut properties: BTreeMap<String, String> = BTreeMap::new();
    properties.insert(
        "tier_groups".to_string(),
        "A={lemma,pos,Inf-Struct};B={}".to_string(),
    );
    let actual = import_as_graphml_string::<TextgridImporter, _>(
        Path::new("tests/data/import/textgrid/input/misalignedLemma"),
        properties,
    )
    .unwrap();

    // Compare the actual output with the expected one
    assert_eq!(
        include_str!("../../../tests/data/import/textgrid/expected/misalignedLemma.graphml").trim(),
        actual.trim()
    );
}
