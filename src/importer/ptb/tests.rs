use std::{collections::BTreeMap, path::Path};

use crate::{importer::ptb::PtbImporter, util::import_as_graphml_string};
use insta::assert_snapshot;

#[test]
fn single_sentence() {
    let properties: BTreeMap<String, String> = BTreeMap::new();

    let actual = import_as_graphml_string(
        PtbImporter::default(),
        Path::new("tests/data/import/ptb/single_sentence"),
        properties,
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn brackets_in_value() {
    let properties: BTreeMap<String, String> = BTreeMap::new();

    let actual = import_as_graphml_string(
        PtbImporter::default(),
        Path::new("tests/data/import/ptb/brackets_in_value"),
        properties,
    )
    .unwrap();

    assert_snapshot!(actual);
}
