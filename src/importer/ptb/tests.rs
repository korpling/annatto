use std::{collections::BTreeMap, path::Path};

use insta::assert_snapshot;

use crate::{importer::ptb::PtbImporter, util::import_as_graphml_string};

#[test]
fn ptb_single_sentence() {
    let properties: BTreeMap<String, String> = BTreeMap::new();

    let actual = import_as_graphml_string::<PtbImporter, _>(
        Path::new("tests/data/import/ptb/single_sentence"),
        properties,
    )
    .unwrap();

    assert_snapshot!(actual);
}
