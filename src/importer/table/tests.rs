use std::path::Path;

use graphannis::{
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::graph::ANNIS_NS;
use insta::assert_snapshot;

use crate::{
    importer::{
        GenericImportConfiguration,
        table::{EmptyLineGroup, ImportTable},
    },
    test_util::{import_as_graphml_string, import_as_graphml_string_with_custom_config},
};

#[test]
fn serialize() {
    let module = ImportTable::default();
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
    let module = ImportTable {
        column_names: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        delimiter: ',',
        quote_char: Some('\''),
        empty_line_group: Some(EmptyLineGroup {
            anno: AnnoKey {
                ns: "default_ns".into(),
                name: "sentence".into(),
            },
            component: Some(AnnotationComponent::new(
                AnnotationComponentType::Coverage,
                ANNIS_NS.into(),
                "".into(),
            )),
        }),
        na: None,
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
fn table_default_config() {
    let actual = import_as_graphml_string(
        ImportTable {
            ..Default::default()
        },
        Path::new("tests/data/import/table/simple/"),
        None,
    );
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}

#[test]
fn table_sentence_span() {
    let m: ImportTable = toml::from_str(r#"empty_line_group = {anno="sentence"}"#).unwrap();
    let actual = import_as_graphml_string(m, Path::new("tests/data/import/table/simple/"), None);
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}

#[test]
fn table_custom_span_component() {
    let m: ImportTable =
        toml::from_str(r#"empty_line_group = {anno="sentence", component = {ctype="Dominance", layer="test", name="sent"}}"#).unwrap();
    let actual = import_as_graphml_string(m, Path::new("tests/data/import/table/simple/"), None);
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}

#[test]
fn table_skip_na() {
    let m: ImportTable = toml::from_str(
        r#"
        delimiter = "\t"
        na = "_"        
        "#,
    )
    .unwrap();
    let actual = import_as_graphml_string(m, Path::new("tests/data/import/table/with-na/"), None);
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}

#[test]
fn custom_generic_config() {
    let m: ImportTable = toml::from_str(
        r#"
        delimiter = "\t"
        na = "_"        
        "#,
    )
    .unwrap();
    let actual = import_as_graphml_string_with_custom_config(
        m,
        Path::new("tests/data/import/table/with-na/"),
        None,
        GenericImportConfiguration::new(
            Some("custom_root".to_string()),
            vec!["csv".to_string(), "tsv".to_string(), "txt".to_string()],
            None,
            Some("custom_ns".to_string()),
        ),
    );
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}
