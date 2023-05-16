use assert_cmd::prelude::*;
use insta::assert_snapshot;
use std::process::Command;

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
        .arg("tests/data/import/empty/empty.ato")
        .output()
        .unwrap();
    cmd.assert().success();

    // Get output and replace version number
    let output = std::str::from_utf8(&output.stderr).unwrap();
    let output = output.replace(env!("CARGO_PKG_VERSION"), "<version>");

    assert_snapshot!(output);
}
