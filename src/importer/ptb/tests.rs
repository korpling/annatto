use std::{collections::BTreeMap, path::Path};

use insta::assert_snapshot;

use crate::{importer::ptb::PtbImporter, util::import_as_graphml_string};

#[test]
fn ptb_oneline() {
    let properties: BTreeMap<String, String> = BTreeMap::new();

    let actual = import_as_graphml_string::<PtbImporter, _>(
        Path::new("tests/data/import/ptb/oneline"),
        properties,
    )
    .unwrap();

    assert_snapshot!(actual);
}
