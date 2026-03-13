mod merge;

use std::path::Path;

use graphannis::AnnotationGraph;
use insta::assert_snapshot;

use crate::{
    StepID,
    exporter::graphml::GraphMLExporter,
    importer::{ImportRunConfiguration, Importer, exmaralda::ImportEXMARaLDA},
    manipulator::{Manipulator, diff::DiffSubgraphs},
    test_util::export_to_string,
    util::update_graph_silent,
};

#[test]
fn deserialize_serialize() {
    let toml_str = r#"
        by = "namespace::alternative_key"
        source_parent = "corpus/subcorpora/a"
        source_component = { ctype = "Ordering", layer = "default_ns", name = "norm" }
        source_key = "norm::norm"
        target_parent = "corpus/subcorpora/b"
        target_component = { ctype = "Ordering", layer = "default_ns", name = "txt" }
        target_key = "txt::txt"
        algorithm = "lcs"
        "#;
    let r: Result<DiffSubgraphs, _> = toml::from_str(toml_str);
    assert!(r.is_ok(), "Could not deserialize: {:?}", r.err().unwrap());
    let diff = r.unwrap();
    assert_snapshot!(toml::to_string(&diff).unwrap());
}

#[test]
fn diff() {
    let import: Result<ImportEXMARaLDA, _> = toml::from_str("");
    assert!(import.is_ok());
    let import = import.unwrap();
    let u = import.import_corpus(
        Path::new("tests/data/graph_op/diff/diff"),
        StepID {
            module_name: "test_import".to_string(),
            path: None,
        },
        ImportRunConfiguration::default(),
        None,
    );
    assert!(u.is_ok());
    let mut update = u.unwrap();
    let g = AnnotationGraph::with_default_graphstorages(true);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    assert!(update_graph_silent(&mut graph, &mut update).is_ok());
    assert!(graph.calculate_all_statistics().is_ok());
    let d: Result<DiffSubgraphs, _> = toml::from_str(
        r#"
        source_parent = "diff/a"
        source_component = { ctype = "Ordering", layer = "annis", name = "dipl" }
        source_key = "dipl::dipl"
        target_parent = "diff/b"
        target_component = { ctype = "Ordering", layer = "annis", name = "norm" }
        target_key = "norm::norm"
        "#,
    );
    assert!(d.is_ok());
    let diff = d.unwrap();
    assert!(
        diff.manipulate_corpus(
            &mut graph,
            Path::new("./"),
            StepID {
                module_name: "test_manip".to_string(),
                path: None
            },
            None
        )
        .is_ok()
    );
    let export: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
    assert!(export.is_ok());
    let export = export.unwrap();
    let actual = export_to_string(&graph, export);
    assert_snapshot!(actual.unwrap());
}

#[test]
fn diff_inverse() {
    let import: Result<ImportEXMARaLDA, _> = toml::from_str("");
    assert!(import.is_ok());
    let import = import.unwrap();
    let u = import.import_corpus(
        Path::new("tests/data/graph_op/diff/diff"),
        StepID {
            module_name: "test_import".to_string(),
            path: None,
        },
        ImportRunConfiguration::default(),
        None,
    );
    assert!(u.is_ok());
    let mut update = u.unwrap();
    let g = AnnotationGraph::with_default_graphstorages(true);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    assert!(update_graph_silent(&mut graph, &mut update).is_ok());
    assert!(graph.calculate_all_statistics().is_ok());
    let d: Result<DiffSubgraphs, _> = toml::from_str(
        r#"
        target_parent = "diff/a"
        target_component = { ctype = "Ordering", layer = "annis", name = "dipl" }
        target_key = "dipl::dipl"
        source_parent = "diff/b"
        source_component = { ctype = "Ordering", layer = "annis", name = "norm" }
        source_key = "norm::norm"
        "#,
    );
    assert!(d.is_ok());
    let diff = d.unwrap();
    let manip = diff.manipulate_corpus(
        &mut graph,
        Path::new("./"),
        StepID {
            module_name: "test_manip".to_string(),
            path: None,
        },
        None,
    );
    assert!(manip.is_ok(), "Err: {:?}", manip.err().unwrap());
    let export: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
    assert!(export.is_ok());
    let export = export.unwrap();
    let actual = export_to_string(&graph, export);
    assert_snapshot!(actual.unwrap());
}
