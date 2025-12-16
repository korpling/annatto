![docs.rs](https://img.shields.io/docsrs/annatto)
[![codecov](https://codecov.io/gh/korpling/annatto/graph/badge.svg?token=51VXZ3IJPA)](https://codecov.io/gh/korpling/annatto)

# Annatto

This software aims to test and convert data within the [RUEG](https://hu.berlin/rueg)
research group at Humboldt-Universität zu Berlin. Tests aim at 
continuously evaluating the state of the [RUEG corpus data](https://zenodo.org/record/3236068)
to early identify issues regarding compatibility, consistency, and 
integrity to facilitate data handling with regard to annotation, releases
and integration. 

For efficiency annatto relies on the [graphANNIS representation](https://korpling.github.io/graphANNIS/docs/v2.2/data-model.html)
and already provides a basic set of data handling modules. We recommend to get acquianted with the [ANNIS Query language](http://korpling.github.io/ANNIS/4/user-guide/aql/index.html)
to better understand the more advanced features of Annatto.

## Installing and running annatto

Annatto is a command line program, which is available pre-compiled for Linux, Windows and macOS.
Download and extract the [latest release file](https://github.com/korpling/annatto/releases/latest) for your platform. 

After extracting the binary to a directory of your choice, you can run the binary by opening a terminal and execute
```bash
<path-to-directory>/annatto
```
on Linux and macOS and 
```bash
<path-to-directory>\annatto.exe
```
on Windows.
If the annatto binary is located in the current working directory, you can also just execute `./annatto` on Linux and macOS and `annatto.exe` on Windows.
In the following examples, the prefix to the path is omitted.

The main usage of annatto is through the command line interface. Run
```bash
annatto --help
```
to get more help on the sub-commands.
The most important command is `annatto run <workflow-file>`, which runs all the modules as defined in the given [workflow] file.

## Modules

Annatto comes with a number of modules, which have different types:

**Importer** modules allow importing files from different formats.
More than one importer can be used in a workflow, but then the corpus data needs
to be merged using one of the merger manipulators.
When running a workflow, the importers are executed first and in parallel.
  

**Graph operation** modules change the imported corpus data.
They are executed one after another (non-parallel) and in the order they have been defined in the workflow.

**Exporter** modules export the data into different formats.
More than one exporter can be used in a workflow.
When running a workflow, the exporters are executed last and in parallel.

To list all available formats (importer, exporter) and graph operations run
```bash
annatto list
```

To show information about modules for the given format or graph operation use
```bash
annatto info <name>
```

The documentation for the modules are also included [here](https://github.com/korpling/annatto/blob/v0.45.0/docs/README.md).

## Creating a workflow file

Annatto workflow files list which importers, graph operations and exporters to execute.
We use an [TOML file](https://toml.io/) with the ending `.toml` to configure the workflow.
TOML files can be as simple as key-value pairs, like `config-key = "config-value"`.
But they allow representing more complex structures, such as lists.
The [TOML website](https://toml.io/) has a great "Quick Tour" section which explains the basics concepts of TOML with examples.

### Import

An import step starts with the header `[[import]]`, and a
configuration value for the key `path` where to read the corpus from and the key `format` which declares in which format the corpus is encoded.
The file path is relative to the workflow file.
Importers also have an additional configuration header, that follows the `[[import]]` section and is marked with the `[import.config]` header.


```toml
[[import]]
path = "textgrid/exampleCorpus/"
format = "textgrid"

[import.config]
tier_groups = { tok = [ "pos", "lemma", "Inf-Struct" ] }
skip_timeline_generation = true
skip_audio = true
skip_time_annotations = true
audio_extension = "wav"
```

You can have more than one importer, and you can simply list all the different importers at the beginning of the workflow file.
An importer always needs to have a configuration header, even if it does not set any specific configuration option.

```toml
[[import]]
path = "a/mycorpus/"
format = "format-a"

[import.config]

[[import]]
path = "b/mycorpus/"
format = "format-b"

[import.config]

[[import]]
path = "c/mycorpus/"
format = "format-c"

[import.config]

# ...
```

### Graph operations

Graph operations use the header `[[graph_op]]` and the key `action` to describe which action to execute.
Since there are no files to import/export, they don't have a `path` configuration.

```toml
[[graph_op]]
action = "check"

[graph_op.config]
# Empty list of tests
tests = []
```

### Export

Exporters work similar to importers, but use the keyword `[[export]]` instead.

```toml
[[export]]
path = "output/exampleCorpus"
format = "graphml"

[export.config]
add_vis = "# no vis"
guess_vis = true
```

### Full example

You cannot mix import, graph operations and export headers. You have to first list all the import steps, then the graph operations and then the export steps.

```toml
[[import]]
path = "conll/ExampleCorpus"
format = "conllu"

[import.config]

[[graph_op]]
action = "check"

[graph_op.config]
report = "list"

[[graph_op.config.tests]]
query = "tok"
expected = [ 1, inf ]
description = "There is at least one token."

[[graph_op.config.tests]]
query = "node ->dep node"
expected = [ 1, inf ]
description = "There is at least one dependency relation."

[[export]]
path = "grapml/"
format = "graphml"

[export.config]
add_vis = "# no vis"
guess_vis = true

```


## Developing annatto

You need to install Rust to compile the project.
We recommend installing the following Cargo subcommands for developing annis-web:

- [cargo-release](https://crates.io/crates/cargo-release) for creating releases
- [cargo-about](https://crates.io/crates/cargo-about) for re-generating the
  third party license file
- [cargo-llvm-cov](https://crates.io/crates/cargo-llvm-cov) for determining the code coverage
- [cargo-insta](https://crates.io/crates/cargo-insta) allows reviewing the test snapshot files
- [cargo-dist](https://crates.io/crates/cargo-dist) for configuring the GitHub actions that create the release binaries.

### Execute tests

You can run the tests with the default `cargo test` command.
To calculate the code coverage, you can use `cargo-llvm-cov`:

```bash
cargo llvm-cov --open --all-features --ignore-filename-regex 'tests?\.rs'
```


### Performing a release

You need to have [`cargo-release`](https://crates.io/crates/cargo-release)
installed to perform a release. Execute the follwing `cargo` command once to
install it.

```bash
cargo install cargo-release cargo-about
```

To perform a release, switch to the main branch and execute:

```bash
cargo release [LEVEL] --execute
```

The [level](https://github.com/crate-ci/cargo-release/blob/HEAD/docs/reference.md#bump-level) should be `patch`, `minor` or `major` depending on the changes made in the release.
Running the release command will also trigger a CI workflow to create release binaries on GitHub.


## Funding

Die Forschungsergebnisse dieser Veröffentlichung wurden gefördert durch die Deutsche Forschungsgemeinschaft (DFG) – SFB 1412, 416591334 sowie FOR 2537, 313607803, GZ LU 856/16-1.

This research was funded by the German Research Foundation (DFG, Deutsche Forschungsgemeinschaft) – SFB 1412, 416591334 and FOR 2537, 313607803, GZ LU 856/16-1.
