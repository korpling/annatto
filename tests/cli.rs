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
    let output = std::str::from_utf8(&output.stderr).unwrap();

    assert_snapshot!(output);
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
    let output = std::str::from_utf8(&output.stderr).unwrap();

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
    let output = std::str::from_utf8(&output.stderr).unwrap();

    assert_snapshot!(output);
}

#[test]
fn load_complex_workflow() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .arg("validate")
        .arg("tests/data/import/workflows/complex_all_attributes.toml")
        .output()
        .unwrap();
    cmd.assert().success();

    // Get output
    let output = std::str::from_utf8(&output.stderr).unwrap();
    assert!(output.is_empty());
}

#[test]
fn load_complex_workflow_attr_ommited() {
    let mut cmd = Command::cargo_bin("annatto").unwrap();

    let output = cmd
        .arg("validate")
        .arg("tests/data/import/workflows/complex_some_attributes.toml")
        .output()
        .unwrap();
    cmd.assert().success();

    // Get output
    let output = std::str::from_utf8(&output.stderr).unwrap();
    dbg!(&output);
    assert!(output.is_empty());
}
