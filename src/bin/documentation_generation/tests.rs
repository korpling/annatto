use std::io::BufWriter;

use insta::assert_snapshot;

use super::*;

#[test]
fn empty_module_list_table() {
    let importers = Vec::default();
    let exporters = Vec::default();
    let graph_ops = Vec::default();

    let actual = module_list_table(&importers, &exporters, &graph_ops);
    assert_snapshot!(actual);
}

#[test]
fn simple_list_table() {
    let actual = module_list_table(
        &vec![ReadFromDiscriminants::GraphML, ReadFromDiscriminants::None],
        &vec![
            WriteAsDiscriminants::GraphML,
            WriteAsDiscriminants::Sequence,
        ],
        &vec![GraphOpDiscriminants::None, GraphOpDiscriminants::Chunk],
    );
    assert_snapshot!(actual);
}

#[test]
fn empty_module_fields() {
    let mut buffer = BufWriter::new(Vec::new());

    write_module_fields(&mut buffer, &Vec::default()).unwrap();

    let actual = String::from_utf8(buffer.into_inner().unwrap()).unwrap();
    assert_eq!("*No Configuration*\n", actual);
}
