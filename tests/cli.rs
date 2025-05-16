use assert_cmd::Command;
use insta::assert_snapshot;
use regex::Regex;
use std::{fs, path::PathBuf};
use tempfile::{tempdir, TempDir};

#[test]
fn show_help() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd.arg("help").output().unwrap();
    cmd.assert().success();

    // Get output and replace version number
    let output = std::str::from_utf8(&output.stdout).unwrap();
    let output = output.replace(env!("CARGO_PKG_VERSION"), "<version>");

    assert_snapshot!(output);
}

#[test]
fn run_empty_conversion() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .arg("run")
        .arg("tests/data/import/empty/empty.toml")
        .output()
        .unwrap();
    cmd.assert().success();

    // Get output
    let output_err = std::str::from_utf8(&output.stderr).unwrap();
    assert_snapshot!(output_err);
    let output = std::str::from_utf8(&output.stdout).unwrap();
    assert_snapshot!(output);
}

#[test]
fn convert_to_itself() {
    // Create temporary folder for test output
    let tmp_out = tempfile::tempdir().unwrap();

    std::env::set_var("TEST_OUTPUT", tmp_out.path().to_string_lossy().as_ref());

    let mut cmd = Command::cargo_bin("annatto").unwrap();

    cmd.arg("run")
        .arg("--env")
        .arg("tests/workflows/convert_to_itself.toml")
        .output()
        .unwrap();
    cmd.assert().success();

    // Input and output files should be the same
    let original = include_str!("data/import/graphml/single_sentence.graphml").trim();
    let converted =
        std::fs::read_to_string(tmp_out.path().join("single_sentence.graphml")).unwrap();
    pretty_assertions::assert_str_eq!(original, converted);
}

#[test]
fn run_empty_conversion_abs_path() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let workflow_file = PathBuf::from("tests/data/import/empty/empty.toml");

    let output = cmd
        .arg("run")
        .arg(
            workflow_file
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref(),
        )
        .output()
        .unwrap();
    cmd.assert().success();

    // Get output
    let output_err = std::str::from_utf8(&output.stderr).unwrap();
    assert_snapshot!(output_err);
    let output = std::str::from_utf8(&output.stdout).unwrap();
    assert_snapshot!(output);
}

#[test]
fn run_failing_conversion() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .arg("run")
        .arg("tests/data/import/failing/failing.toml")
        .output()
        .unwrap();
    cmd.assert().failure();

    // Get output
    let output_err = std::str::from_utf8(&output.stderr).unwrap();
    assert_snapshot!(output_err);
    let output = std::str::from_utf8(&output.stdout).unwrap();
    assert_snapshot!(output);
}

#[test]
fn load_complex_workflow() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .arg("validate")
        .arg("tests/workflows/complex_all_attributes.toml")
        .output()
        .unwrap();
    cmd.assert().success();

    // Get output
    let output_err = std::str::from_utf8(&output.stderr).unwrap();
    assert_snapshot!(output_err);
    let output = std::str::from_utf8(&output.stdout).unwrap();
    assert_snapshot!(output);
}

#[test]
fn load_complex_workflow_attr_ommited() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .arg("validate")
        .arg("tests/workflows/complex_some_attributes.toml")
        .output()
        .unwrap();
    cmd.assert().success();

    // Get output
    let output_err = std::str::from_utf8(&output.stderr).unwrap();
    assert_snapshot!(output_err);
    let output = std::str::from_utf8(&output.stdout).unwrap();
    assert_snapshot!(output);
}

#[test]
fn list_modules() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd.env("NO_COLOR", "1").arg("list").output().unwrap();
    cmd.assert().success();

    let output = std::str::from_utf8(&output.stdout).unwrap();
    assert_snapshot!(output);
}

#[test]
fn module_info() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .env("NO_COLOR", "1")
        .arg("info")
        .arg("xlsx")
        .output()
        .unwrap();
    cmd.assert().success();

    let output = std::str::from_utf8(&output.stdout).unwrap();

    assert_snapshot!(output);
}

#[test]
fn module_info_relannis() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .env("NO_COLOR", "1")
        .arg("info")
        .arg("relannis")
        .output()
        .unwrap();
    cmd.assert().success();

    let output = std::str::from_utf8(&output.stdout).unwrap();

    assert_snapshot!(output);
}

#[test]
fn graph_op_info() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .env("NO_COLOR", "1")
        .arg("info")
        .arg("merge")
        .output()
        .unwrap();

    let output = std::str::from_utf8(&output.stdout).unwrap();

    assert_snapshot!(output);
}

#[test]
fn unknown_module_info() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .env("NO_COLOR", "1")
        .arg("info")
        .arg("thiswillnotexist")
        .output()
        .unwrap();

    let output = std::str::from_utf8(&output.stdout).unwrap();

    assert_snapshot!(output);
}

#[test]
fn write_documentation() {
    // Create an output directory for the documentation
    let output_dir = tempdir().unwrap();

    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .arg("documentation")
        .arg(output_dir.path())
        .output()
        .unwrap();

    let output = std::str::from_utf8(&output.stdout).unwrap();

    assert_snapshot!(output);

    // Also check that the files have been created
    assert_eq!(true, output_dir.path().join("README.md").is_file());
    assert_eq!(true, output_dir.path().join("importers").is_dir());
    assert_eq!(true, output_dir.path().join("exporters").is_dir());
    assert_eq!(true, output_dir.path().join("graph_ops").is_dir());
}

#[test]
fn run_and_serialize() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();
    let tmp_dir = TempDir::new().unwrap();
    let output_file = tmp_dir.path().join("exported_workflow.toml");
    let output = cmd
        .arg("run")
        .arg("tests/data/workflow/short.toml")
        .arg("--save")
        .arg(format!("{}", output_file.to_string_lossy()).as_str())
        .output();
    assert!(output.is_ok());
    assert!(output_file.exists());
    let workflow_str = fs::read_to_string(output_file.as_path()).unwrap();
    assert_snapshot!(Regex::new(r#"[0-9]+\.[0-9]+\.[0-9]+"#)
        .unwrap()
        .replace(&workflow_str, "<VERSION>"));
}
