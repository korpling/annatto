use super::*;
use std::{fs, path::Path};

use graphannis::AnnotationGraph;
use insta::assert_snapshot;
use tempfile::TempDir;
use zip::ZipArchive;

use crate::{
    core::update_graph_silent,
    importer::{
        exmaralda::ImportEXMARaLDA, file_nodes::CreateFileNodes, xlsx::ImportSpreadsheet, Importer,
    },
};

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
        zip_copy_from: Some("copy/path/".into()),
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

#[test]
fn zip_with_linked_files_custom() {
    let g = AnnotationGraph::with_default_graphstorages(false);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    let u1 = ImportSpreadsheet::default().import_corpus(
        Path::new("tests/data/export/graphml/linked-files/src/data"),
        StepID {
            module_name: "test_import".to_string(),
            path: None,
        },
        None,
    );
    assert!(u1.is_ok());
    let file_linker: Result<CreateFileNodes, _> = toml::from_str("corpus_name = \"data\"");
    assert!(file_linker.is_ok());
    let u2 = file_linker.unwrap().import_corpus(
        Path::new("tests/data/export/graphml/linked-files/config_files/data"),
        StepID {
            module_name: "link_files".to_string(),
            path: None,
        },
        None,
    );
    assert!(u2.is_ok(), "Error linking files: {:?}", u2.err());
    assert!(update_graph_silent(&mut graph, &mut u1.unwrap()).is_ok());
    assert!(update_graph_silent(&mut graph, &mut u2.unwrap()).is_ok());
    let export = GraphMLExporter {
        zip: true,
        zip_copy_from: Some("tests/data/export/graphml/linked-files/config_files/".into()),
        ..Default::default()
    }
    .export_corpus(
        &graph,
        Path::new("tests/data/export/graphml/linked-files/target/"),
        StepID {
            module_name: "test_export".to_string(),
            path: None,
        },
        None,
    );
    assert!(export.is_ok(), "Error exporting: {:?}", export.err());
    let zip_path = Path::new("tests/data/export/graphml/linked-files/target/data.zip");
    assert!(zip_path.exists());
    let zf = fs::File::open(zip_path);
    assert!(zf.is_ok());
    let a = ZipArchive::new(zf.unwrap());
    assert!(a.is_ok());
    let archive = a.unwrap();
    assert_snapshot!(archive.file_names().sorted().join("\n"));
}
