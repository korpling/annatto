use std::io::BufWriter;

use super::*;
use graphannis::AnnotationGraph;
use pretty_assertions::assert_eq;

#[test]
fn single_speaker() {
    let mut properties: BTreeMap<String, String> = BTreeMap::new();
    properties.insert(
        "tier_groups".to_string(),
        "tok={lemma,pos,Inf-Struct}".to_string(),
    );
    properties.insert("map_timeline".to_string(), "false".to_string());
    let importer = TextgridImporter::default();

    let mut u = importer
        .import_corpus(
            &PathBuf::from("tests/data/textgrid/in/singleSpeaker"),
            &properties,
            None,
        )
        .unwrap();
    let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
    g.apply_update(&mut u, |_| {}).unwrap();

    let mut buf = BufWriter::new(Vec::new());
    graphannis_core::graph::serialization::graphml::export(&g, None, &mut buf, |_| {}).unwrap();
    let bytes = buf.into_inner().unwrap();
    let actual = String::from_utf8(bytes).unwrap();

    // Compare the actual output with the expected one
    assert_eq!(
        include_str!("../../../tests/data/textgrid/out/singleSpeaker.graphml"),
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
    let importer = TextgridImporter::default();

    let mut u = importer
        .import_corpus(
            &PathBuf::from("tests/data/textgrid/in/twoSpeakers"),
            &properties,
            None,
        )
        .unwrap();
    let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
    g.apply_update(&mut u, |_| {}).unwrap();

    let mut buf = BufWriter::new(Vec::new());
    graphannis_core::graph::serialization::graphml::export(&g, None, &mut buf, |_| {}).unwrap();
    let bytes = buf.into_inner().unwrap();
    let actual = String::from_utf8(bytes).unwrap();

    // Compare the actual output with the expected one
    assert_eq!(
        include_str!("../../../tests/data/textgrid/out/twoSpeakers.graphml"),
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
    let importer = TextgridImporter::default();

    let mut u = importer
        .import_corpus(
            &PathBuf::from("tests/data/textgrid/in/misalignedLemma"),
            &properties,
            None,
        )
        .unwrap();
    let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
    g.apply_update(&mut u, |_| {}).unwrap();

    let mut buf = BufWriter::new(Vec::new());
    graphannis_core::graph::serialization::graphml::export(&g, None, &mut buf, |_| {}).unwrap();
    let bytes = buf.into_inner().unwrap();
    let actual = String::from_utf8(bytes).unwrap();

    // Compare the actual output with the expected one
    assert_eq!(
        include_str!("../../../tests/data/textgrid/out/twoSpeakers.graphml"),
        actual
    );
}

#[test]
fn needs_tier_groups_property() {
    let properties: BTreeMap<String, String> = BTreeMap::new();

    let importer = TextgridImporter::default();
    let result = importer.import_corpus(
        &PathBuf::from("tests/data/textgrid/in/singleSpeaker"),
        &properties,
        None,
    );
    assert_eq!(true, result.is_err());
}
