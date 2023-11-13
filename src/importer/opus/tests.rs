use std::path::Path;

use insta::assert_snapshot;

use crate::{importer::opus::ImportOpusLinks, util::import_as_graphml_string};

#[test]
fn test_generic_xml() {
    let actual = import_as_graphml_string(
        ImportOpusLinks::default(),
        Path::new("tests/data/import/opus/"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}
