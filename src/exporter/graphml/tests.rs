use super::*;
use std::path::Path;

use graphannis::AnnotationGraph;
use insta::assert_snapshot;
use tempfile::TempDir;

use crate::importer::{exmaralda::ImportEXMARaLDA, Importer};

#[test]
fn serialize() {
    let module = GraphMLExporter::default();
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
    let module = GraphMLExporter {
        add_vis: Some("# just add this random comment, alright?".to_string()),
        guess_vis: true,
        stable_order: true,
        zip: true,
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
fn export_as_zip_with_files() {
    let step_id = StepID {
        module_name: "export_graphml".to_string(),
        path: None,
    };
    let importer = ImportEXMARaLDA::default();
    let mut updates = importer
        .import_corpus(
            Path::new("tests/data/import/exmaralda/clean/import/exmaralda"),
            step_id.clone(),
            None,
        )
        .unwrap();
    let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
    g.apply_update(&mut updates, |_| {}).unwrap();

    // Export the annotation graph, but zip the content
    let mut exporter = GraphMLExporter::default();
    exporter.zip = true;

    let output_path = TempDir::new().unwrap();

    exporter
        .export_corpus(&g, output_path.path(), step_id, None)
        .unwrap();
    // The output directory should contain a single ZIP file
    let zip_file_path = output_path.path().join("exmaralda.zip");
    let zip_file = std::fs::File::open(&zip_file_path).unwrap();
    assert_eq!(true, zip_file_path.is_file());
    // Read the ZIP file and check its contents
    let zip = zip::ZipArchive::new(zip_file).unwrap();
    let files: Vec<_> = zip.file_names().sorted().collect();
    assert_eq!(
        vec![
            "exmaralda.graphml",
            "tests/data/import/exmaralda/clean/import/exmaralda/test_file.wav",
        ],
        files
    );
}

#[test]
fn export_graphml_with_vis() {
    let step_id = StepID {
        module_name: "export_graphml".to_string(),
        path: None,
    };
    let importer = ImportEXMARaLDA::default();
    let mut updates = importer
        .import_corpus(
            Path::new("tests/data/import/exmaralda/clean/import/exmaralda"),
            step_id.clone(),
            None,
        )
        .unwrap();
    let mut g = AnnotationGraph::with_default_graphstorages(false).unwrap();
    g.apply_update(&mut updates, |_| {}).unwrap();

    // Export the annotation graph, but zip the content
    let mut exporter = GraphMLExporter::default();
    exporter.guess_vis = true;
    exporter.stable_order = true;

    let output_path = TempDir::new().unwrap();

    exporter
        .export_corpus(&g, output_path.path(), step_id, None)
        .unwrap();

    // Read the generated GraphML file
    let result_file_path = output_path.path().join("exmaralda.graphml");
    let graphml = std::fs::read_to_string(result_file_path).unwrap();
    assert_snapshot!(graphml);
}
