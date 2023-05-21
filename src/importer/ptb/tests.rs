use std::path::Path;

use crate::{importer::ptb::PtbImporter, util::import_as_graphml_string};
use insta::assert_snapshot;

const PTB_DEFAULT_VIS_CONFIG: &str = r#"
[context]
default = 5
sizes = [0, 1, 2, 5, 10]

[view]
page_size = 10

[[visualizers]]
vis_type = "kwic"
display_name = "kwic"
visibility = "permanent"

[[visualizers]]
element = "node"
layer = "syntax"
vis_type = "tree"
display_name = "tree"
visibility = "hidden"


[[visualizers]]
vis_type = "kwic"
display_name = "kwic"
visibility = "permanent"

[[visualizers]]
element = "node"
layer = "syntax"
vis_type = "tree"
display_name = "tree"
visibility = "hidden"
"#;

#[test]
fn single_sentence() {
    let actual = import_as_graphml_string(
        PtbImporter::default(),
        Path::new("tests/data/import/ptb/single_sentence"),
        Some(PTB_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn brackets_in_value() {
    let actual = import_as_graphml_string(
        PtbImporter::default(),
        Path::new("tests/data/import/ptb/brackets_in_value"),
        Some(PTB_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}

#[test]
fn multiple_documents() {
    let actual = import_as_graphml_string(
        PtbImporter::default(),
        Path::new("tests/data/import/ptb/multiple_documents"),
        Some(PTB_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}
