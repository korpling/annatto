use std::{path::Path, sync::mpsc};

use graphannis::update::GraphUpdate;
use insta::assert_snapshot;

use crate::{importer::Importer, test_util::import_as_graphml_string, Module};

use super::ImportCoNLLU;

#[test]
fn test_conll_fail_invalid() {
    let import = ImportCoNLLU::default();
    let import_path = Path::new("tests/data/import/conll/invalid");
    let job = import.import_corpus(import_path, import.step_id(Some(import_path)), None);
    assert!(job.is_err());
    assert_snapshot!(job.err().unwrap().to_string());
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            &mut u,
            import_path.join("test_file.conllu").as_path(),
            import_path.join("test_file").to_str().unwrap().to_string(),
            &None
        )
        .is_err());
}

#[test]
fn test_conll_fail_invalid_heads() {
    let import = ImportCoNLLU::default();
    let import_path = Path::new("tests/data/import/conll/invalid-heads/");
    let (sender, _receiver) = mpsc::channel();
    let job = import.import_corpus(import_path, import.step_id(None), Some(sender));
    assert!(job.is_err());
    assert_snapshot!(job.err().unwrap().to_string());
}

#[test]
fn test_conll_fail_cyclic() -> Result<(), Box<dyn std::error::Error>> {
    let import = ImportCoNLLU::default();
    let import_path = Path::new("tests/data/import/conll/cyclic-deps/");
    let job = import.import_corpus(import_path, import.step_id(None), None);
    assert!(job.is_ok());
    Ok(())
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
