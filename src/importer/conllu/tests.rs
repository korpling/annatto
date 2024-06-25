use std::{path::Path, sync::mpsc};

use graphannis::{graph::AnnoKey, update::GraphUpdate};
use insta::assert_snapshot;

use crate::{test_util::import_as_graphml_string, ReadFrom, StepID};

use super::ImportCoNLLU;

#[test]
fn test_conll_fail_invalid() {
    let import = ReadFrom::CoNLLU(ImportCoNLLU::default());
    let import_path = Path::new("tests/data/import/conll/invalid");
    let step_id = StepID::from_importer_module(&import, Some(import_path.to_path_buf()));
    let job = import
        .reader()
        .import_corpus(import_path, step_id.clone(), None);
    assert!(job.is_err());
    assert_snapshot!(job.err().unwrap().to_string());
    let mut u = GraphUpdate::default();
    let import = ImportCoNLLU::default();
    assert!(import
        .import_document(
            &step_id,
            &mut u,
            import_path.join("test_file.conllu").as_path(),
            import_path.join("test_file").to_str().unwrap().to_string(),
            &None
        )
        .is_err());
}

#[test]
fn test_conll_fail_invalid_heads() {
    let import = ReadFrom::CoNLLU(ImportCoNLLU::default());
    let import_path = Path::new("tests/data/import/conll/invalid-heads/");
    let step_id = StepID::from_importer_module(&import, Some(import_path.to_path_buf()));
    let (sender, _receiver) = mpsc::channel();
    let job = import
        .reader()
        .import_corpus(import_path, step_id, Some(sender));
    assert!(job.is_err());
    assert_snapshot!(job.err().unwrap().to_string());
}

#[test]
fn test_conll_fail_cyclic() -> Result<(), Box<dyn std::error::Error>> {
    let import = ReadFrom::CoNLLU(ImportCoNLLU::default());
    let import_path = Path::new("tests/data/import/conll/cyclic-deps/");
    let step_id = StepID::from_importer_module(&import, Some(import_path.to_path_buf()));

    let job = import.reader().import_corpus(import_path, step_id, None);
    assert!(job.is_ok());
    Ok(())
}

#[test]
fn comments_and_sentence_annos() {
    let actual = import_as_graphml_string(
        ImportCoNLLU::default(),
        Path::new("tests/data/import/conll/comments/"),
        None,
    );
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}

#[test]
fn custom_comments() {
    let actual = import_as_graphml_string(
        ImportCoNLLU {
            comment_anno: AnnoKey {
                ns: "custom".into(),
                name: "comment_key".into(),
            },
        },
        Path::new("tests/data/import/conll/comments/"),
        None,
    );
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}

#[test]
fn basic() {
    let actual = import_as_graphml_string(
        ImportCoNLLU::default(),
        Path::new("tests/data/import/conll/valid/"),
        None,
    );
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}
