use std::path::Path;

use graphannis::AnnotationGraph;
use insta::assert_snapshot;

use crate::{
    StepID,
    exporter::treetagger::ExportTreeTagger,
    importer::{
        Importer,
        treetagger::{AttributeDecoding, ImportTreeTagger},
    },
    test_util::{export_to_string, import_as_graphml_string},
};

const TT_DEFAULT_VIS_CONFIG: &str = r#"
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
vis_type = "grid"
display_name = "grid"
"#;

#[test]
fn serialize() {
    let module = ImportTreeTagger::default();
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
    let module: ImportTreeTagger = toml::from_str("column_names = [\"a\", \"b\", \"c\"]\nfile_encoding = \"latin-1\"\nattribute_decoding = \"none\"").unwrap();
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

#[test]
fn simple_token() {
    let actual = import_as_graphml_string(
        ImportTreeTagger::default(),
        Path::new("tests/data/import/treetagger/token_only"),
        Some(TT_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn encoding_latin() {
    let mut importer = ImportTreeTagger::default();
    importer.file_encoding = Some("Latin1".into());
    let actual = import_as_graphml_string(
        importer,
        Path::new("tests/data/import/treetagger/latin1"),
        Some(TT_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn disable_attribute_encoding() {
    let mut importer = ImportTreeTagger::default();
    importer.attribute_decoding = AttributeDecoding::Entities;
    let should_fail = import_as_graphml_string(
        importer,
        Path::new("tests/data/import/treetagger/unescaped_attribute/"),
        Some(TT_DEFAULT_VIS_CONFIG),
    );
    assert!(should_fail.is_err());

    let mut importer: ImportTreeTagger = ImportTreeTagger::default();
    importer.attribute_decoding = AttributeDecoding::None;
    let actual = import_as_graphml_string(
        importer,
        Path::new("tests/data/import/treetagger/unescaped_attribute/"),
        Some(TT_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn complex_attribute_names() {
    let importer = ImportTreeTagger::default();
    let path = Path::new("tests/data/import/treetagger/complex_attribute_names/");
    let exporter = ExportTreeTagger::default();

    let step_id = StepID {
        module_name: "import_under_test".to_string(),
        path: None,
    };
    let mut u = importer
        .import_corpus(path.as_ref(), step_id.clone(), None)
        .unwrap();
    let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
    g.apply_update(&mut u, |_| {}).unwrap();

    let result = export_to_string(&g, exporter).unwrap();

    assert_snapshot!(result);
}
