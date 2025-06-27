use std::path::Path;

use graphannis::{
    AnnotationGraph,
    update::{GraphUpdate, UpdateEvent},
};
use insta::assert_snapshot;
use tempfile::TempDir;

use super::*;
use crate::{
    ExporterStep, ImporterStep, StepID, importer::saltxml::ImportSaltXml,
    test_util::compare_graphs, util::example_generator,
};

#[test]
fn serialize() {
    let module = ExportSaltXml::default();
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

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
    let mut u = GraphUpdate::new();
    example_generator::create_corpus_structure_two_documents(&mut u);
    for d in ["root/doc1", "root/doc2"] {
        example_generator::create_tokens(&mut u, Some(d));
        example_generator::make_span(
            &mut u,
            &format!("{d}#span1"),
            &[&format!("{d}#tok1"), &format!("{d}#tok2")],
            true,
        );
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("{d}#span1"),
            target_node: d.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: "PartOf".to_string(),
            component_name: "".to_string(),
        })
        .unwrap();
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: format!("{d}#span1"),
            anno_ns: "default_ns".into(),
            anno_name: "phrase".into(),
            anno_value: "this example".into(),
        })
        .unwrap();
        u.add_event(UpdateEvent::AddNodeLabel {
            node_name: format!("{d}#span1"),
            anno_ns: "annis".into(),
            anno_name: "layer".into(),
            anno_value: "test-layer".into(),
        })
        .unwrap();
    }

    let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
    g.apply_update(&mut u, |_msg| {}).unwrap();

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

    // Also check the existince and content of the created document graph files
    let p1 = corpus_dir.join("root/doc1.salt");
    assert_eq!(true, p1.is_file());
    let p2 = corpus_dir.join("root/doc2.salt");
    assert_eq!(true, p2.is_file());

    let doc1 = std::fs::read_to_string(p1).unwrap();
    assert_snapshot!("doc1", doc1);

    let doc2 = std::fs::read_to_string(p2).unwrap();
    assert_snapshot!("doc2", doc2);
}

#[test]
fn export_example_segmentation() {
    let mut u = GraphUpdate::new();
    example_generator::create_corpus_structure_simple(&mut u);
    example_generator::create_multiple_segmentations(&mut u, "root/doc1");

    let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
    g.apply_update(&mut u, |_msg| {}).unwrap();

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

    let doc = std::fs::read_to_string(p).unwrap();
    assert_snapshot!(doc);
}

#[test]
fn export_token_non_trivial_chars() {
    let mut u = GraphUpdate::new();
    example_generator::create_corpus_structure_simple(&mut u);

    let token_strings = [
        ("", "Anöther", " "),
        ("", "example", " "),
        ("", "for", " "),
        ("", "a", " "),
        ("'", "Tractaͤtlein", "'"),
    ];
    for (i, (ws_before, t, ws_after)) in token_strings.iter().enumerate() {
        let ws_before = if ws_before.is_empty() {
            None
        } else {
            Some(*ws_before)
        };
        let ws_after = if ws_after.is_empty() {
            None
        } else {
            Some(*ws_after)
        };
        example_generator::create_token_node(
            &mut u,
            &format!("root/doc1#tok{i}"),
            t,
            ws_before,
            ws_after,
            Some("root/doc1"),
        );
    }

    // add the order relations
    for i in 0..token_strings.len() {
        u.add_event(UpdateEvent::AddEdge {
            source_node: format!("root/doc1#tok{i}"),
            target_node: format!("root/doc1#tok{}", i + 1),
            layer: ANNIS_NS.to_string(),
            component_type: "Ordering".to_string(),
            component_name: "".to_string(),
        })
        .unwrap();
    }

    let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
    g.apply_update(&mut u, |_msg| {}).unwrap();

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

    // Also check the existince and content of the created document graph files
    let p = corpus_dir.join("root/doc1.salt");
    assert_eq!(true, p.is_file());

    let doc = std::fs::read_to_string(p).unwrap();
    assert_snapshot!(doc);
}

#[test]
fn import_export_sample_sentence() {
    let importer: ImportSaltXml = toml::from_str(r#"missing_anno_ns_from_layer = false"#).unwrap();
    let exporter = ExportSaltXml::default();

    // Import the example project
    let path = Path::new("./tests/data/import/salt/SaltSampleCorpus");
    let orig_import_step = ImporterStep {
        module: crate::ReadFrom::SaltXml(importer),
        path: path.to_path_buf(),
    };
    let mut updates = orig_import_step.execute(None).unwrap();
    let mut original_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
    original_graph.apply_update(&mut updates, |_| {}).unwrap();

    // Export to SaltXML project, read it again and then compare the annotation graphs
    let tmp_outputdir = TempDir::new().unwrap();
    let output_dir = tmp_outputdir.path().join("SaltSampleCorpus");
    std::fs::create_dir(&output_dir).unwrap();
    let exporter = crate::WriteAs::SaltXml(exporter);
    let export_step = ExporterStep {
        module: exporter,
        path: output_dir.clone(),
    };
    export_step.execute(&original_graph, None).unwrap();

    let importer: ImportSaltXml = toml::from_str(r#"missing_anno_ns_from_layer = false"#).unwrap();
    let second_import_step = ImporterStep {
        module: crate::ReadFrom::SaltXml(importer),
        path: output_dir.clone(),
    };
    let mut updates = second_import_step.execute(None).unwrap();
    let mut written_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();

    written_graph.apply_update(&mut updates, |_| {}).unwrap();

    compare_graphs(&original_graph, &written_graph);
}

#[test]
fn import_export_dialog_demo() {
    let importer: ImportSaltXml = toml::from_str(r#"missing_anno_ns_from_layer = false"#).unwrap();
    let exporter = ExportSaltXml::default();

    // Import the example project
    let path = Path::new("./tests/data/import/salt/dialog.demo");
    let orig_import_step = ImporterStep {
        module: crate::ReadFrom::SaltXml(importer),
        path: path.to_path_buf(),
    };
    let mut updates = orig_import_step.execute(None).unwrap();
    let mut original_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();
    original_graph.apply_update(&mut updates, |_| {}).unwrap();

    // Export to SaltXML project, read it again and then compare the annotation graphs
    let tmp_outputdir = TempDir::new().unwrap();
    let output_dir = tmp_outputdir.path().join("dialog.demo");
    std::fs::create_dir(&output_dir).unwrap();
    let exporter = crate::WriteAs::SaltXml(exporter);
    let export_step = ExporterStep {
        module: exporter,
        path: output_dir.clone(),
    };
    export_step.execute(&original_graph, None).unwrap();

    let importer: ImportSaltXml = toml::from_str(r#"missing_anno_ns_from_layer = false"#).unwrap();
    let second_import_step = ImporterStep {
        module: crate::ReadFrom::SaltXml(importer),
        path: output_dir.clone(),
    };
    let mut updates = second_import_step.execute(None).unwrap();
    let mut written_graph = AnnotationGraph::with_default_graphstorages(false).unwrap();

    written_graph.apply_update(&mut updates, |_| {}).unwrap();

    compare_graphs(&original_graph, &written_graph);
}
