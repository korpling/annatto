use std::path::Path;

use graphannis::AnnotationGraph;
use insta::assert_snapshot;

use crate::{
    exporter::graphml::GraphMLExporter,
    importer::{
        DefaultImportConfiguration, GenericImportConfiguration, Importer, elan::ImportELAN,
    },
    test_util::export_to_string,
};

#[test]
fn basic() {
    let path = Path::new("tests/data/import/elan/");
    let import = ImportELAN::default();
    let u = import.import_corpus(
        path,
        crate::StepID {
            module_name: "test_elan".to_string(),
            path: None,
        },
        import.default_configuration(),
        None,
    );
    assert!(u.is_ok(), "Err: {}", u.err().unwrap());
    let g = AnnotationGraph::with_default_graphstorages(false);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
    let export: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
    assert!(export.is_ok());
    assert_snapshot!(export_to_string(&graph, export.unwrap()).unwrap());
}

#[test]
fn with_segmentations() {
    let path = Path::new("tests/data/import/elan/");
    let import = ImportELAN {
        segmentations: ["Referenztext_W".to_string()].into_iter().collect(),
        ..Default::default()
    };
    let u = import.import_corpus(
        path,
        crate::StepID {
            module_name: "test_elan".to_string(),
            path: None,
        },
        import.default_configuration(),
        None,
    );
    assert!(u.is_ok(), "Err: {}", u.err().unwrap());
    let g = AnnotationGraph::with_default_graphstorages(false);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
    let export: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
    assert!(export.is_ok());
    assert_snapshot!(export_to_string(&graph, export.unwrap()).unwrap());
}

#[test]
fn customized_generic_config() {
    let path = Path::new("tests/data/import/elan/");
    let import = ImportELAN::default();
    let config = GenericImportConfiguration::new(
        Some("custom_root".to_string()),
        import
            .default_file_extensions()
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        None,
        Some("custom_ns".to_string()),
    );
    let u = import.import_corpus(
        path,
        crate::StepID {
            module_name: "test_elan".to_string(),
            path: None,
        },
        config,
        None,
    );
    assert!(u.is_ok(), "Err: {}", u.err().unwrap());
    let g = AnnotationGraph::with_default_graphstorages(false);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    assert!(graph.apply_update(&mut u.unwrap(), |_| {}).is_ok());
    let export: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
    assert!(export.is_ok());
    assert_snapshot!(export_to_string(&graph, export.unwrap()).unwrap());
}
