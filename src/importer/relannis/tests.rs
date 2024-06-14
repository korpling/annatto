use std::sync::mpsc::{self};

use super::*;
use crate::test_util::import_as_graphml_string_2;
use csv::StringRecord;
use insta::assert_snapshot;
use pretty_assertions::assert_eq;

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
fn import_order_relation() {
    let corpus_path = Path::new("tests/data/import/relannis/testComplexMapSOrderRelation/");
    let actual = import_as_graphml_string_2(
        ImportRelAnnis::default(),
        corpus_path,
        Some(
            r#"
        [[visualizers]]
        element = "node"
        vis_type = "grid"
        display_name = "annotations"
        visibility = "permanent"
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
fn import_legacy_format() {
    let corpus_path = Path::new("tests/data/import/relannis/dialog.demo/");
    let actual = import_as_graphml_string_2(
        ImportRelAnnis::default(),
        corpus_path,
        Some(
            r#"
            [[visualizers]]
            element = "node"
            vis_type = "grid"
            display_name = "speakers (grid)"
            visibility = "permanent"

            [visualizers.mappings]
            hide_tok = "true"
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
fn import_corpus_with_escaped_id() {
    let corpus_path = Path::new("tests/data/import/relannis/testIDEscape/");
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
            element = "edge"
            layer = "%37%23%3D%2B%7D%7D%C3%A4%3F%C3%B6%3B"
            vis_type = "arch_dependency"
            display_name = "deps"
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
fn import_corpus_with_duplicated_document() {
    let corpus_path = Path::new("tests/data/import/relannis/DuplicatedDocumentName/");

    let (sender, receiver) = mpsc::channel();
    let actual = import_as_graphml_string_2(
        ImportRelAnnis::default(),
        corpus_path,
        None,
        false,
        Some(sender),
    )
    .unwrap();
    assert_snapshot!(actual);
    let warnings: Vec<_> = receiver
        .into_iter()
        .filter(|msg| match msg {
            crate::workflow::StatusMessage::Warning(_) => true,
            _ => false,
        })
        .collect();
    assert_eq!(1, warnings.len());
    assert_eq!(
        r#"Warning("duplicated document name \"doc1\" detected: will be renamed to \"doc1_duplicated_document_name_2\"")"#,
        format!("{:?}", warnings[0])
    );
}

#[test]
fn unescape_field() {
    let path = Path::new("node.annis");
    let record = StringRecord::from(vec![
        "0",
        "0",
        "1",
        "default_ns",
        "sTok1",
        "0",
        "12",
        "0",
        "0",
        "0",
        "NULL",
        "NULL",
        r#"a\\b\\\\c\n\r\n\t𐌰"#,
        "TRUE",
    ]);
    let actual = get_field(&record, 12, "span", path).unwrap().unwrap();
    assert_eq!(actual, "a\\b\\\\c\n\r\n\t𐌰");
}

#[test]
fn invalid_column() {
    let path = Path::new("node.annis");
    let record = StringRecord::from(vec!["0", "0", "1", "default_ns", "sTok1"]);
    let actual = get_field(&record, 12, "span", path);
    assert!(actual.is_err());
    assert_eq!(
        actual.err().unwrap().to_string(),
        "missing column at position 12 (span) in file node.annis"
    );
}

#[test]
fn fail_on_null_column() {
    let path = Path::new("node.annis");
    let record = StringRecord::from(vec!["0", "NULL", "NULL", "default_ns", "sTok1"]);
    let actual = get_field_not_null(&record, 1, "span", path);
    assert!(actual.is_err());
    assert_eq!(
        actual.err().unwrap().to_string(),
        "unexpected value NULL in column 1 (span) in file node.annis at line <unknown>"
    );
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

    let expected = TextKey {
        id: 123,
        corpus_ref: None,
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
