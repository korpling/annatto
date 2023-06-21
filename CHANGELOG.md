# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## Changes

- CLI binary renamed from `annatto-cli` to `annatto`
- To execute a workflow file, use `annatto run <workflow-file>`
- module properties are now struct attributes of the importer, manipulator, or exporter, which facilitates deserialization and also use (undefined properties are no longer accepted, required properties cannot be ommited)
- only TOML workflow files are now supported, xml workflows can no longer be processed
- TOML support adds a command `annatto validate <workflow-file>` that checks if a worklow description can be deserialized to an internal workflow
- not all modules have a default implementation anymore (path attributes have no default value that makes sense)
- there is a default operation for each step type. Import: Create empty corpus, Manipulation: Do nothing, Export: Write GraphML
- map properties of some modules (such as `tier_group` for importing Textgrid) are no longer String codings, since TOML supports providing maps directly
- flattened TOML for workflow files
- TOML workflows: module config has to be singled out in separate table
- `check` tests are now configured in main workflow as TOML fragment
- `check` report table contains number of matches in case of failure

### Added

- collected errors in status messages `Failed` are now all reported at the end
  of the job
- an annotation mapper can create annotations from existing annotations using
  AQL for defining target nodes
- New command `show-documentation` for CLI, which starts a browser with the user
  guide.
- after running `check`, the the test results can be printed as a table (default: off)
- `check` displays matching nodes for tests in new verbose mode
- `check` now comes with a higher level test ("Layer test") that is internally converted into atomic aql tests. The test can be applied to nodes and edges. It tests if a layer exists and only valid annotation values have been used.
- using flag `--env` allows to resolve environmental variables in workflow definitions which enables the use of template workflow definitions

### Fixed

- fixed panics caused by undefined attributes in tier tag or missing speaker table / wrong speaker id
- exmaralda import did not properly forward errors through the status sender, which it now does

## [0.2.0] - 2023-04-27

### Added

- added import module for exmaralda partitur files
- set annis::layer with speaker name when importing exmaralda files
- spreadsheet import builds regular ANNIS coverage-based model
- import CoNLL-U files

### Fixed

- fix character buffer in exmaralda import
- order names are no longer part of the guessed visualisation when exporting GraphML
- if audio file linked in an exmaralda file cannot be found, no audio source will be linked
- exmaralda import: Multiple tlis with the same time value are now merged into a single tli token
- Upgrade to quick-xml 0.28 to avoid issues in future versions of Rust.
- exmaralda: catch flipped time values (start >= end)

## [0.1.0] - 2023-04-12

### Added

- allow to leak graph updates to text file
- import textgrid, ptb, graphml, corpus annotations (metadata), spreadsheets
- check documents with list of AQL queries and expected results
- merge multiple imports to single graph
- merge policy for merge: fail on error, forward error (corrupted graph), drop subgraph (document) with errors
- apply single combined update after imports are finished to avoid multiple
- calls to apply_update
- replace annotation names, namespaces, move annotations, delete annotations (re)
- export graphml
