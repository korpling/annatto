use std::{collections::BTreeSet, path::Path, sync::mpsc};

use graphannis::{
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
};
use graphannis_core::graph::DEFAULT_NS;
use insta::assert_snapshot;

use crate::{
    ImporterStep, ReadFrom, StepID,
    importer::{
        DefaultComponents, DefaultImportConfiguration, GenericImportConfiguration,
        conllu::default_comment_key,
    },
    test_util::{import_as_graphml_string, import_as_graphml_string_with_custom_config},
    util::graphupdate::UpdateWriter,
};

use super::ImportCoNLLU;

#[test]
fn serialize() {
    let module = ImportCoNLLU::default();
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

#[test]
fn serialize_custom() {
    let module = ImportCoNLLU {
        comment_anno: AnnoKey {
            name: "metadata".into(),
            ns: "default_ns".into(),
        },
        multi_tok: Some(AnnoKey {
            name: "norm".into(),
            ns: "norm".into(),
        }),
        labeled_targets: true,
    };
    let serialization = toml::to_string(&module);
    assert!(
        serialization.is_ok(),
        "Serialization failed: {:?}",
        serialization.err()
    );
    assert_snapshot!(serialization.unwrap());
}

#[test]
fn test_conll_fail_invalid() {
    let import = ReadFrom::CoNLLU(ImportCoNLLU::default());
    let import_path = Path::new("tests/data/import/conll/invalid");
    let generic_config = import.reader().default_configuration();
    let import_step = ImporterStep {
        module: import,
        path: import_path.to_path_buf(),
        description: None,
        generic_config,
    };
    let job = import_step.execute(None);
    assert!(job.is_err());
    assert_snapshot!(job.err().unwrap().to_string());
    let import = ImportCoNLLU::default();
    let mut u = UpdateWriter::from(import.default_configuration());
    let step_id = StepID::from_importer_step(&import_step);
    let run = import.import_document(
        &step_id,
        &mut u,
        import_path.join("test_file.conllu").as_path(),
        import_path.join("test_file").to_str().unwrap().to_string(),
    );
    assert!(run.is_err());
    assert_snapshot!(run.err().unwrap());
}

#[test]
fn fail_missing_column() {
    let import = ReadFrom::CoNLLU(ImportCoNLLU::default());
    let import_path = Path::new("tests/data/import/conll/missing-column/");
    let generic_config = import.reader().default_configuration();
    let import_step = ImporterStep {
        module: import,
        path: import_path.to_path_buf(),
        description: None,
        generic_config,
    };
    let (sender, _receiver) = mpsc::channel();
    let job = import_step.execute(Some(sender));
    assert!(job.is_err());
    assert_snapshot!(job.err().unwrap().to_string());
}

#[test]
fn test_conll_fail_invalid_heads() {
    let import = ReadFrom::CoNLLU(ImportCoNLLU::default());
    let import_path = Path::new("tests/data/import/conll/invalid-heads/");
    let generic_config = import.reader().default_configuration();
    let import_step = ImporterStep {
        module: import,
        path: import_path.to_path_buf(),
        description: None,
        generic_config,
    };
    let (sender, _receiver) = mpsc::channel();
    let job = import_step.execute(Some(sender));
    assert!(job.is_err());
    assert_snapshot!(job.err().unwrap().to_string());
}

#[test]
fn test_conll_fail_cyclic() -> Result<(), Box<dyn std::error::Error>> {
    let import = ReadFrom::CoNLLU(ImportCoNLLU::default());
    let import_path = Path::new("tests/data/import/conll/cyclic-deps/");
    let generic_config = import.reader().default_configuration();
    let import_step = ImporterStep {
        module: import,
        path: import_path.to_path_buf(),
        description: None,
        generic_config,
    };

    let job = import_step.execute(None);
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
            ..Default::default()
        },
        Path::new("tests/data/import/conll/comments/"),
        None,
    );
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}

#[test]
fn multi_token() {
    let actual = import_as_graphml_string(
        ImportCoNLLU {
            multi_tok: Some(AnnoKey {
                name: "norm".into(),
                ns: "default_ns".into(),
            }),
            ..Default::default()
        },
        Path::new("tests/data/import/conll/multi-tok/"),
        None,
    );
    assert!(
        actual.is_ok(),
        "Error in multi-tok import: {:?}",
        actual.err()
    );
    assert_snapshot!(actual.unwrap());
}

#[test]
fn deser_default() {
    let toml_str = "";
    let mprt: Result<ImportCoNLLU, _> = toml::from_str(toml_str);
    assert!(mprt.is_ok());
    assert!(mprt.unwrap().comment_anno == default_comment_key());
}

#[test]
fn deser_custom() {
    let toml_str = "comment_anno = { ns = \"custom_ns\", name = \"custom_name\" }";
    let mprt: Result<ImportCoNLLU, _> = toml::from_str(toml_str);
    assert!(mprt.is_ok(), "Error when deserializing: {:?}", mprt.err());
    let import = mprt.unwrap();
    assert!(
        import.comment_anno
            == AnnoKey {
                ns: "custom_ns".into(),
                name: "custom_name".into()
            }
    );
}

#[test]
fn deser_multi() {
    let toml_str = "multi_tok = { ns = \"default_ns\", name = \"norm\"}";
    let mprt: Result<ImportCoNLLU, _> = toml::from_str(toml_str);
    assert!(mprt.is_ok(), "Error when deserializing: {:?}", mprt.err());
    let import = mprt.unwrap();
    assert!(import.multi_tok.is_some());
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

#[test]
fn customized_generic_config() {
    let importer = ImportCoNLLU::default();
    let config = GenericImportConfiguration::new(
        Some("custom_root".to_string()),
        vec!["conllu".to_string()],
        Some(BTreeSet::from(["website_example".to_string()])),
        crate::importer::ModelConfiguration::new(
            "custom_ns".to_string(),
            DefaultComponents::default()
                .with_coverage(AnnotationComponent::new(
                    AnnotationComponentType::Coverage,
                    DEFAULT_NS.to_string(),
                    "custom_cov".to_string(),
                ))
                .with_part_of(AnnotationComponent::new(
                    AnnotationComponentType::PartOf,
                    DEFAULT_NS.to_string(),
                    "custom_part_of".to_string(),
                ))
                .with_pointing(AnnotationComponent::new(
                    AnnotationComponentType::Pointing,
                    DEFAULT_NS.to_string(),
                    "syntax".to_string(),
                )),
        ),
    );
    let actual = import_as_graphml_string_with_custom_config(
        importer,
        Path::new("tests/data/import/conll/valid/"),
        None,
        config,
    );
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}

#[test]
fn labeled_targets() {
    let importer = ImportCoNLLU {
        labeled_targets: true,
        ..Default::default()
    };
    let config = GenericImportConfiguration::new(
        Some("custom_root".to_string()),
        vec!["conllu".to_string()],
        Some(BTreeSet::from(["website_example".to_string()])),
        crate::importer::ModelConfiguration::new(
            "custom_ns".to_string(),
            DefaultComponents::default()
                .with_pointing(importer.preset_default_pointing().cloned().unwrap()),
        ),
    );
    let actual = import_as_graphml_string_with_custom_config(
        importer,
        Path::new("tests/data/import/conll/valid/"),
        None,
        config,
    );
    assert!(actual.is_ok());
    assert_snapshot!(actual.unwrap());
}
