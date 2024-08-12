use graphannis::{update::GraphUpdate, AnnotationGraph};
use insta::assert_snapshot;
use tempfile::TempDir;

use super::*;
use crate::{util::example_generator, StepID};

#[test]
fn export_corpus_structure() {
    let mut updates = GraphUpdate::new();
    example_generator::create_corpus_structure_two_documents(&mut updates);

    // Add some meta data to the documents and also add them to a layer
    updates
        .add_event(graphannis::update::UpdateEvent::AddNodeLabel {
            node_name: "root/doc1".to_string(),
            anno_ns: "test".to_string(),
            anno_name: "description".to_string(),
            anno_value: "A demo corpus".to_string(),
        })
        .unwrap();
    updates
        .add_event(graphannis::update::UpdateEvent::AddNodeLabel {
            node_name: "root/doc1".to_string(),
            anno_ns: "annis".to_string(),
            anno_name: "layer".to_string(),
            anno_value: "test-layer".to_string(),
        })
        .unwrap();
    updates
        .add_event(graphannis::update::UpdateEvent::AddNodeLabel {
            node_name: "root/doc1".to_string(),
            anno_ns: "test".to_string(),
            anno_name: "author".to_string(),
            anno_value: "unknown".to_string(),
        })
        .unwrap();
    updates
        .add_event(graphannis::update::UpdateEvent::AddNodeLabel {
            node_name: "root/doc2".to_string(),
            anno_ns: "test".to_string(),
            anno_name: "description".to_string(),
            anno_value: "Another demo corpus".to_string(),
        })
        .unwrap();
    updates
        .add_event(graphannis::update::UpdateEvent::AddNodeLabel {
            node_name: "root/doc2".to_string(),
            anno_ns: "annis".to_string(),
            anno_name: "layer".to_string(),
            anno_value: "test-layer".to_string(),
        })
        .unwrap();

    let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
    g.apply_update(&mut updates, |_msg| {}).unwrap();

    let exporter = ExportSaltXml {};
    let output_path = TempDir::new().unwrap();
    let corpus_dir = output_path.path().join("root");
    std::fs::create_dir(&corpus_dir).unwrap();

    let step_id = StepID {
        module_name: "export_saltxml".to_string(),
        path: Some(corpus_dir.clone()),
    };

    exporter
        .export_corpus(&g, &corpus_dir, step_id.clone(), None)
        .unwrap();

    // There should be a saltProject.salt file
    let p = corpus_dir.join("saltProject.salt");
    assert_eq!(true, p.is_file());
    let result = std::fs::read_to_string(p).unwrap();
    assert_snapshot!(result);
}

#[test]
fn export_example_token() {
    let mut updates = GraphUpdate::new();
    example_generator::create_corpus_structure_simple(&mut updates);
    example_generator::create_tokens(&mut updates, Some("root/doc1"));
    example_generator::make_span(
        &mut updates,
        "root/doc1#span1",
        &["root/doc1#tok1", "root/doc1#tok2"],
        true,
    );
    updates
        .add_event(graphannis::update::UpdateEvent::AddNodeLabel {
            node_name: "root/doc1#span1".into(),
            anno_ns: "default_ns".into(),
            anno_name: "phrase".into(),
            anno_value: "this example".into(),
        })
        .unwrap();

    let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
    g.apply_update(&mut updates, |_msg| {}).unwrap();

    let exporter = ExportSaltXml {};
    let output_path = TempDir::new().unwrap();
    let corpus_dir = output_path.path().join("root");
    std::fs::create_dir(&corpus_dir).unwrap();

    let step_id = StepID {
        module_name: "export_saltxml".to_string(),
        path: Some(corpus_dir.clone()),
    };

    exporter
        .export_corpus(&g, &corpus_dir, step_id.clone(), None)
        .unwrap();

    // There should be a saltProject.salt file
    let project_path = corpus_dir.join("saltProject.salt");
    assert_eq!(true, project_path.is_file());

    // Also check the existince and content of the created document graph file
    let p = corpus_dir.join("root/doc1.salt");
    assert_eq!(true, p.is_file());

    let result = std::fs::read_to_string(p).unwrap();
    assert_snapshot!(result);
}
