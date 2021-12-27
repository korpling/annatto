use std::path::PathBuf;

use pepper::workflow;

#[test]
fn exmaralda_import() {
    let test_path = PathBuf::from("tests/corpora/exb2graphml.pepper");
    workflow::execute_from_file(&test_path, None).unwrap();
}
