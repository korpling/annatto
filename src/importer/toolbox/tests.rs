use std::{fs, path::Path};

use graphannis::model::{AnnotationComponent, AnnotationComponentType};
use insta::assert_snapshot;

use crate::importer::{
    DefaultComponents, DefaultImportConfiguration, GenericImportConfiguration,
    toolbox::ImportFLToolbox,
};

#[test]
fn serialize_custom() {
    let module = ImportFLToolbox {
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
    let imp: Result<ImportFLToolbox, _> = toml::from_str(toml_str.as_str());
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
    let imp: Result<ImportFLToolbox, _> = toml::from_str(toml_str.as_str());
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
fn custom_update_config() {
    let ts = fs::read_to_string("tests/data/import/toolbox/build.toml");
    assert!(ts.is_ok(), "Could not read workflow: {:?}", ts.err());
    let toml_str = ts.unwrap();
    let imp: Result<ImportFLToolbox, _> = toml::from_str(toml_str.as_str());
    assert!(imp.is_ok(), "Error occurred: {:?}", imp.err());
    let importer = imp.unwrap();
    let config = GenericImportConfiguration {
        root_as: Some("custom_root".to_string()),
        extensions: importer
            .default_file_extensions()
            .iter()
            .map(ToString::to_string)
            .collect(),
        documents: None,
        default_ns: Some("custom_ns".to_string()),
        default_components: Some(DefaultComponents::new(
            Some(AnnotationComponent::new(
                AnnotationComponentType::Ordering,
                "custom-ord".to_string(),
                "custom-ord-name".to_string(),
            )),
            Some(AnnotationComponent::new(
                AnnotationComponentType::Coverage,
                "custom-cov".to_string(),
                "custom-cov-name".to_string(),
            )),
            Some(AnnotationComponent::new(
                AnnotationComponentType::PartOf,
                "custom-part-of".to_string(),
                "custom-part-of-name".to_string(),
            )),
            None,
            None,
        )),
    };
    let graphml_is = crate::test_util::import_as_graphml_string_with_custom_config(
        importer,
        Path::new("tests/data/import/toolbox/"),
        None,
        config,
    );
    assert!(
        graphml_is.is_ok(),
        "Failed to import test file: {:?}",
        graphml_is.err()
    );
    assert_snapshot!(graphml_is.unwrap());
}
