use std::{collections::BTreeMap, io::BufWriter, path::PathBuf};

use graphannis::AnnotationGraph;
use insta::assert_snapshot;

use crate::importer::Importer;

use super::PtbImporter;

#[test]
fn ptb_oneline() {
    let properties: BTreeMap<String, String> = BTreeMap::new();

    let importer = PtbImporter::default();

    let mut u = importer
        .import_corpus(
            &PathBuf::from("tests/data/import/ptb/oneline"),
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

    assert_snapshot!(actual);
}
