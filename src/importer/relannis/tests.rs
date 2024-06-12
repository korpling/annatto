use super::*;
use crate::test_util::import_as_graphml_string_2;
use insta::assert_snapshot;

#[test]
fn import_salt_sample_relannis() {
    let actual = import_as_graphml_string_2(
        ImportRelAnnis::default(),
        Path::new("tests/data/import/relannis/SaltSampleCorpus/"),
        Some(
            r#"
        [[visualizers]]
        element = "node"
        vis_type = "kwic"
        display_name = "Key Word in Context"
        visibility = "permanent"

        [[visualizers]]
        element = "node"
        layer = "syntax"
        vis_type="tree"
        display_name="syntax"
        visibility = "hidden"

        [visualizers.mappings]
        node_key = "const"

        [[visualizers]]
        element = "edge"
        layer = "default_ns"
        vis_type = "discourse"
        display_name = "anaphoric"
        visibility = "hidden"

        [[visualizers]]
        element = "node"
        vis_type = "grid"
        display_name = "annotations"
        visibility = "hidden"
        "#,
        ),
        false,
        None,
    )
    .unwrap();

    assert_snapshot!(actual);
}
