use std::path::Path;

use insta::assert_snapshot;

use crate::{importer::xml::ImportXML, test_util::import_as_graphml_string};

#[test]
fn inline() {
    let actual = import_as_graphml_string(
        ImportXML::default(),
        Path::new("tests/data/import/generic_xml/inline/"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn standoff() {
    let ser = "text_from_attribute = { token = \"text\" }\nclosing_default = \" \"";
    let importer: Result<ImportXML, _> = toml::from_str(ser);
    assert!(
        importer.is_ok(),
        "Deserialization error: {:?}",
        importer.err()
    );
    let actual = import_as_graphml_string(
        importer.unwrap(),
        Path::new("tests/data/import/generic_xml/standoff/"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}
