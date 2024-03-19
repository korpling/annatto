use annatto::runtime::EnvVars;
use assert_cmd::prelude::*;
use insta::assert_snapshot;
use std::{path::PathBuf, process::Command};

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
    std::env::set_var(EnvVars::InMemory.to_string(), false.to_string());

    let mut cmd = Command::cargo_bin("annatto").unwrap();

    cmd.arg("run")
        .arg("--env")
        .arg("tests/workflows/convert_to_itself.toml")
        .output()
        .unwrap();
    cmd.assert().success();

    // Input and output files should be the same
    let original = include_str!("data/import/graphml/single_sentence.graphml");
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
