use graphannis::{update::GraphUpdate, AnnotationGraph};
use insta::assert_snapshot;
use tempfile::TempDir;

use super::*;
use crate::{util::example_generator, StepID};

#[test]
fn export_corpus_structure() {
    let mut updates = GraphUpdate::new();
    example_generator::create_corpus_structure_two_documents(&mut updates);
    let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
    g.apply_update(&mut updates, |_msg| {}).unwrap();

    let exporter = ExportSaltXml {};
    let output_path = TempDir::new().unwrap();

    let step_id = StepID {
        module_name: "export_saltxml".to_string(),
        path: Some(output_path.path().to_path_buf()),
    };

    exporter
        .export_corpus(&g, output_path.as_ref(), step_id.clone(), None)
        .unwrap();

    // There should be a saltProject.salt file
    let p = output_path.path().join("saltProject.salt");
    assert_eq!(true, p.is_file());
    let result = std::fs::read_to_string(p).unwrap();
    assert_snapshot!(result);
}
