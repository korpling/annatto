use std::path::Path;

use insta::assert_snapshot;

use crate::{importer::opus::ImportOpusLinks, test_util::import_as_graphml_string};

#[test]
fn serialize() {
    let module = ImportOpusLinks::default();
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

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
