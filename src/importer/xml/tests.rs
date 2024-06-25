use std::path::Path;

use insta::assert_snapshot;

use crate::{importer::xml::ImportXML, test_util::import_as_graphml_string};

#[test]
fn test_generic_xml() {
    let actual = import_as_graphml_string(
        ImportXML {},
        Path::new("tests/data/import/generic_xml/"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}
