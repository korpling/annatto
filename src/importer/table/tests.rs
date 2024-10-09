use std::path::Path;

use insta::assert_snapshot;

use crate::{importer::table::ImportTable, test_util::import_as_graphml_string};

#[test]
fn table_default_config() {
    let actual = import_as_graphml_string(
        ImportTable {
            ..Default::default()
        },
        Path::new("tests/data/import/table/simple/"),
        None,
    );
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}

#[test]
fn table_sentence_span() {
    let m: ImportTable =
        toml::from_str(r#"empty_line_group = {anno="sentence", value="S"}"#).unwrap();
    let actual = import_as_graphml_string(m, Path::new("tests/data/import/table/simple/"), None);
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}
