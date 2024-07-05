use std::path::Path;

use insta::assert_snapshot;

use super::*;
use crate::test_util;

#[test]
fn read_salt_sample_corpus() {
    let importer = ImportSaltXml::default();
    let actual = test_util::import_as_graphml_string(
        importer,
        Path::new("tests/data/import/salt/SaltSampleCorpus"),
        None,
    )
    .unwrap();
    assert_snapshot!(actual);
}

#[test]
fn read_salt_with_timeline() {
    let importer = ImportSaltXml::default();
    let actual = test_util::import_as_graphml_string(
        importer,
        Path::new("tests/data/import/salt/dialog.demo"),
        None,
    )
    .unwrap();
    assert_snapshot!(actual);
}
