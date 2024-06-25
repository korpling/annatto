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
