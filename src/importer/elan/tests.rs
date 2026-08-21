use std::path::Path;

use graphannis::AnnotationGraph;
use insta::assert_snapshot;

use crate::{
    exporter::graphml::GraphMLExporter,
    importer::{Importer, elan::ImportELAN},
    test_util::export_to_string,
};

#[test]
fn basic() {
    let path = Path::new("tests/data/import/elan/");
    let import = ImportELAN {};
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
