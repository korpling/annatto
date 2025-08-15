use std::io::BufWriter;

use annatto::manipulator::{chunker::Chunk, no_op::NoOp};
use facet_reflect::Peek;
use insta::assert_snapshot;
use tempfile::tempdir;

use super::*;

#[test]
fn empty_module_list_table() {
    let importers = Vec::default();
    let exporters = Vec::default();
    let graph_ops = Vec::default();

    let output_dir = tempdir().unwrap();

    write_module_list_table(output_dir.path(), &importers, &exporters, &graph_ops).unwrap();
    let actual = std::fs::read_to_string(output_dir.path().join("README.md")).unwrap();
    assert_snapshot!(actual);
}

#[test]
fn simple_list_table() {
    let output_dir = tempdir().unwrap();

    let example_graph_ops: Vec<_> = [
        GraphOp::None(NoOp::default()),
        GraphOp::Chunk(Chunk::default()),
    ]
    .into_iter()
    .map(|m| {
        Peek::new(&m)
            .into_enum()
            .unwrap()
            .active_variant()
            .unwrap()
            .clone()
    })
    .collect();

    write_module_list_table(
        output_dir.path(),
        &[ReadFromDiscriminants::GraphML, ReadFromDiscriminants::None],
        &[
            WriteAsDiscriminants::GraphML,
            WriteAsDiscriminants::Sequence,
        ],
        &example_graph_ops,
    )
    .unwrap();
    let actual = std::fs::read_to_string(output_dir.path().join("README.md")).unwrap();

    assert_snapshot!(actual);
}

#[test]
fn none_importer_file() {
    let output_dir = tempdir().unwrap();

    write_importer_files(&[ReadFromDiscriminants::None], output_dir.path()).unwrap();
    let actual =
        std::fs::read_to_string(output_dir.path().join("importers").join("none.md")).unwrap();

    assert_snapshot!(actual);
}

#[test]
fn graphml_exporter_file() {
    let output_dir = tempdir().unwrap();

    write_exporter_files(&[WriteAsDiscriminants::GraphML], output_dir.path()).unwrap();
    let actual =
        std::fs::read_to_string(output_dir.path().join("exporters").join("graphml.md")).unwrap();

    assert_snapshot!(actual);
}

#[test]
fn none_graph_op_file() {
    let output_dir = tempdir().unwrap();
    let example_variants: Vec<_> = [GraphOp::None(NoOp::default())]
        .into_iter()
        .map(|m| {
            Peek::new(&m)
                .into_enum()
                .unwrap()
                .active_variant()
                .unwrap()
                .clone()
        })
        .collect();
    write_graph_op_files(&example_variants, output_dir.path()).unwrap();
    let actual =
        std::fs::read_to_string(output_dir.path().join("graph_ops").join("none.md")).unwrap();

    assert_snapshot!(actual);
}

#[test]
fn empty_module_fields() {
    let mut buffer = BufWriter::new(Vec::new());

    write_module_fields(&mut buffer, &Vec::default()).unwrap();

    let actual = String::from_utf8(buffer.into_inner().unwrap()).unwrap();
    assert_eq!("*No Configuration*\n", actual);
}

#[test]
fn simple_module_fields() {
    let mut buffer = BufWriter::new(Vec::new());

    let m1 = ModuleConfiguration {
        name: "test".to_string(),
        description: "A test configuration.".to_string(),
    };
    let m2 = ModuleConfiguration {
        name: "enabled".to_string(),
        description: "Whether this module is *enabled*.".to_string(),
    };
    let empty_description = ModuleConfiguration {
        name: "empty_description".to_string(),
        description: String::default(),
    };

    write_module_fields(&mut buffer, &[m1, empty_description, m2]).unwrap();

    let actual = String::from_utf8(buffer.into_inner().unwrap()).unwrap();

    assert_snapshot!(actual);
}
