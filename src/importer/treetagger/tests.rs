use std::path::Path;

use insta::assert_snapshot;

use crate::{importer::treetagger::TreeTaggerImporter, util::import_as_graphml_string};

const TT_DEFAULT_VIS_CONFIG: &str = r#"
[context]
default = 5
sizes = [0, 1, 2, 5, 10]

[view]
page_size = 10

[[visualizers]]
vis_type = "kwic"
display_name = "kwic"
visibility = "permanent"
"#;

#[test]
fn simple_token() {
    let actual = import_as_graphml_string(
        TreeTaggerImporter::default(),
        Path::new("tests/data/import/treetagger/token_only"),
        Some(TT_DEFAULT_VIS_CONFIG),
    )
    .unwrap();

    assert_snapshot!(actual);
}
