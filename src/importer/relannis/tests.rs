use super::*;
use crate::test_util::import_as_graphml_string_2;
use insta::assert_snapshot;

#[test]
fn import_salt_sample_relannis() {
    let corpus_path = Path::new("tests/data/import/relannis/SaltSampleCorpus/");
    let actual = import_as_graphml_string_2(
        ImportRelAnnis::default(),
        corpus_path,
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

    let path_to_remove =
        pathdiff::diff_paths(std::env::current_dir().unwrap(), corpus_path).unwrap();
    let path_to_remove = path_to_remove.to_str().unwrap();
    insta::with_settings!({filters => vec![
        (path_to_remove, "[PROJECT_DIR]"),
    ]}, {
        assert_snapshot!(actual);
    });
}

#[test]
fn text_property_key_serializer() {
    let expected = TextProperty {
        segmentation: "".to_string(),
        corpus_id: 123,
        text_id: 1,
        val: 42,
    };

    // Serialize and unserialize the key, check they are the same
    let key = expected.create_key();
    let actual = TextProperty::parse_key(&key).unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn text_key_serializer() {
    let expected = TextKey {
        id: 123,
        corpus_ref: Some(42),
    };

    // Serialize and unserialize the key, check they are the same
    let key = expected.create_key();
    let actual = TextKey::parse_key(&key).unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn node_by_text_entry_serializer() {
    let expected = NodeByTextEntry {
        corpus_ref: 123,
        node_id: 42,
        text_id: 1,
    };

    // Serialize and unserialize the key, check they are the same
    let key = expected.create_key();
    let actual = NodeByTextEntry::parse_key(&key).unwrap();

    assert_eq!(actual, expected);
}
