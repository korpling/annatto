use std::{fs, path::Path};

use insta::assert_snapshot;

use crate::importer::fltb::FLToolbox;

#[test]
fn serialize_custom() {
    let module = FLToolbox {
        span: ["sentence".to_string(), "clause".to_string()]
            .into_iter()
            .collect(),
        ignore: ["comment".to_string()].into_iter().collect(),
        globals: ["id".to_string()].into_iter().collect(),
        explicit_null: false,
    };
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

#[test]
fn core_functionality() {
    let ts = fs::read_to_string("tests/data/import/toolbox/build.toml");
    assert!(ts.is_ok(), "Could not read workflow: {:?}", ts.err());
    let toml_str = ts.unwrap();
    let imp: Result<FLToolbox, _> = toml::from_str(toml_str.as_str());
    assert!(imp.is_ok(), "Error occurred: {:?}", imp.err());
    let importer = imp.unwrap();
    let graphml_is = crate::test_util::import_as_graphml_string(
        importer,
        Path::new("tests/data/import/toolbox/"),
        None,
    );
    assert!(
        graphml_is.is_ok(),
        "Failed to import test file: {:?}",
        graphml_is.err()
    );
    assert_snapshot!(graphml_is.unwrap());
}

#[test]
fn explicit_null() {
    let ts = fs::read_to_string("tests/data/import/toolbox/build-explicit-null.toml");
    assert!(ts.is_ok(), "Could not read workflow: {:?}", ts.err());
    let toml_str = ts.unwrap();
    let imp: Result<FLToolbox, _> = toml::from_str(toml_str.as_str());
    assert!(imp.is_ok(), "Error occurred: {:?}", imp.err());
    let importer = imp.unwrap();
    let graphml_is = crate::test_util::import_as_graphml_string(
        importer,
        Path::new("tests/data/import/toolbox/"),
        None,
    );
    assert!(
        graphml_is.is_ok(),
        "Failed to import test file: {:?}",
        graphml_is.err()
    );
    assert_snapshot!(graphml_is.unwrap());
}
