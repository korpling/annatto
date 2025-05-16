use std::path::Path;

use graphannis::{
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::graph::ANNIS_NS;
use insta::assert_snapshot;

use crate::{
    importer::table::{EmptyLineGroup, ImportTable},
    test_util::import_as_graphml_string,
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
