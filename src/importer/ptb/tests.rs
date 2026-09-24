use std::path::Path;

use crate::{
    importer::{GenericImportConfiguration, ptb::ImportPTB},
    test_util::{import_as_graphml_string, import_as_graphml_string_with_custom_config},
};
use insta::assert_snapshot;

const PTB_DEFAULT_VIS_CONFIG: &str = r#"
[context]
default = 5
sizes = [0, 1, 2, 5, 10]

[view]
page_size = 10

[[visualizers]]
vis_type = "kwic"
display_name = "kwic"
visibility = "permanent"

[[visualizers]]
element = "node"
layer = "syntax"
vis_type = "tree"
display_name = "tree"
visibility = "hidden"


[[visualizers]]
vis_type = "kwic"
display_name = "kwic"
visibility = "permanent"

[[visualizers]]
element = "node"
layer = "syntax"
vis_type = "tree"
display_name = "tree"
visibility = "hidden"
"#;

#[test]
fn serialize() {
    let module = ImportPTB::default();
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

#[test]
fn serialize_custom() {
    let module = ImportPTB {
        edge_delimiter: Some("-".to_string()),
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
fn single_sentence() {
    let actual = import_as_graphml_string(
        ImportPTB::default(),
        Path::new("tests/data/import/ptb/single_sentence"),
        Some(PTB_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn brackets_in_value() {
    let actual = import_as_graphml_string(
        ImportPTB::default(),
        Path::new("tests/data/import/ptb/brackets_in_value"),
        Some(PTB_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn multiple_documents() {
    let actual = import_as_graphml_string(
        ImportPTB::default(),
        Path::new("tests/data/import/ptb/multiple_documents"),
        Some(PTB_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn with_edge_functions() {
    let actual = import_as_graphml_string(
        ImportPTB {
            edge_delimiter: Some("-".to_string()),
        },
        Path::new("tests/data/import/ptb/with_edge_functions"),
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn custom_generic_config() {
    let actual = import_as_graphml_string_with_custom_config(
        ImportPTB {
            edge_delimiter: Some("-".to_string()),
        },
        Path::new("tests/data/import/ptb/with_edge_functions"),
        None,
        GenericImportConfiguration::new(
            Some("custom_root".to_string()),
            vec!["ptb".to_string(), "txt".to_string()],
            None,
            Some("custom_ns".to_string()),
        ),
    )
    .unwrap();

    assert_snapshot!(actual);
}
