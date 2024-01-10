use std::{env, path::Path, sync::mpsc};

use graphannis::update::GraphUpdate;
use insta::{assert_display_snapshot, assert_snapshot};
use itertools::Itertools;

use crate::{
    importer::Importer, progress::ProgressReporter, test_util::import_as_graphml_string_2,
    workflow::StatusMessage, Module,
};

use super::ImportEXMARaLDA;

#[test]
fn timeline_fail() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-corrupt_timeline/import/";
    let (sender, _receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), import.step_id(None), Some(sender));
    assert!(r.is_err());
    assert_display_snapshot!(r.err().unwrap());
    let document_path = "./tests/data/import/exmaralda/fail-corrupt_timeline/import/test_doc.exb";
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            "import/test_doc",
            Path::new(document_path),
            &mut u,
            &ProgressReporter::new(None, import.step_id(None), 1).unwrap(),
            &None,
        )
        .is_err());
}

#[test]
fn category_fail() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-no_category/";
    let (sender, _receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), import.step_id(None), Some(sender));
    assert!(r.is_err());
    assert_display_snapshot!(r.err().unwrap());
    let document_path = Path::new(import_path).join("test_doc.exb");
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            "fail-no_category/test_doc",
            document_path.as_path(),
            &mut u,
            &ProgressReporter::new(None, import.step_id(None), 1).unwrap(),
            &None
        )
        .is_err());
}

#[test]
fn speaker_fail() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-no_speaker/";
    let (sender, _receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), import.step_id(None), Some(sender));
    assert!(r.is_err());
    assert_display_snapshot!(r.err().unwrap());
    let document_path = Path::new(import_path).join("test_doc.exb");
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            "fail-no_speaker/test_doc",
            document_path.as_path(),
            &mut u,
            &ProgressReporter::new(None, import.step_id(None), 1).unwrap(),
            &None
        )
        .is_err());
}

#[test]
fn undefined_speaker_fail() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-undefined_speaker/";
    let (sender, _receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), import.step_id(None), Some(sender));
    assert!(r.is_err());
    assert_display_snapshot!(r.err().unwrap());
    let document_path = Path::new(import_path).join("test_doc.exb");
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            "fail-undefined_speaker/test_doc",
            document_path.as_path(),
            &mut u,
            &ProgressReporter::new(None, import.step_id(None), 1).unwrap(),
            &None
        )
        .is_err());
}

#[test]
fn unknown_tli_fail() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-unknown_tli/";
    let (sender, _receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), import.step_id(None), Some(sender));
    assert!(r.is_err());
    assert_display_snapshot!(r.err().unwrap());
    let document_path = Path::new(import_path).join("test_doc.exb");
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            "fail-unknown_tli/test_doc",
            document_path.as_path(),
            &mut u,
            &ProgressReporter::new(None, import.step_id(None), 1).unwrap(),
            &None
        )
        .is_err());
}

#[test]
fn bad_timevalue_fail() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-bad_timevalue/";
    let (sender, _receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), import.step_id(None), Some(sender));
    assert!(r.is_err());
    assert_display_snapshot!(r.err().unwrap());
    let document_path = Path::new(import_path).join("test_doc.exb");
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            "fail-bad_timevalue/test_doc",
            document_path.as_path(),
            &mut u,
            &ProgressReporter::new(None, import.step_id(None), 1).unwrap(),
            &None
        )
        .is_err());
}

#[test]
fn underspec_event_fail() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-no_start_no_end/";
    let (sender, _receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), import.step_id(None), Some(sender));
    assert!(r.is_err());
    assert_display_snapshot!(r.err().unwrap());
}

#[test]
fn invalid_fail() {
    let import = ImportEXMARaLDA::default();
    let import_path = "./tests/data/import/exmaralda/fail-invalid/import/";
    let (sender, _receiver) = mpsc::channel();
    let r = import.import_corpus(Path::new(import_path), import.step_id(None), Some(sender));
    assert!(r.is_err());
    assert_display_snapshot!(r.err().unwrap());
    let document_path = "./tests/data/import/exmaralda/fail-invalid/import/test_doc_invalid.exb";
    let mut u = GraphUpdate::default();
    assert!(import
        .import_document(
            "import/test_doc_invalid",
            Path::new(document_path),
            &mut u,
            &ProgressReporter::new(None, import.step_id(None), 1).unwrap(),
            &None
        )
        .is_err());
}

#[test]
fn import() {
    let r = run_test("./tests/data/import/exmaralda/clean/import/", 0);
    assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    assert_snapshot!(r.unwrap());
}

#[test]
fn broken_audio_pass() {
    let r = run_test("./tests/data/import/exmaralda/broken_audio/import/", 1);
    assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    assert_snapshot!(r.unwrap());
}

#[test]
fn missing_type_attr_pass() {
    let r = run_test("./tests/data/import/exmaralda/pass-no_tier_type/import/", 9);
    assert_eq!(r.is_ok(), true, "Probing core test result {:?}", r);
    assert_snapshot!(r.unwrap());
}

fn run_test(
    import_path: &str,
    expected_warnings_count: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let (sender, receiver) = mpsc::channel();
    let actual = import_as_graphml_string_2(
        ImportEXMARaLDA::default(),
        env::current_dir()?.join(import_path), // IMPORTANT: test with absolute paths, this is what Annatto does at runtime
        None,
        true,
        Some(sender),
    )?;
    let warnings = receiver
        .into_iter()
        .filter(|m| matches!(m, StatusMessage::Warning(..)))
        .collect_vec();
    assert_eq!(
        expected_warnings_count,
        warnings.len(),
        "Unexpected amount of warnings: {:?}",
        warnings
    );
    Ok(actual)
}
