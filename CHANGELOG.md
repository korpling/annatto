# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added simple chunker module based on
  [text-splitter](https://crates.io/crates/text-splitter).
- `check` can write check report to file 

### Changed

- The `check` module can now query the `AnnotationGraph` directly without using
  the `CorpusStorageManager`.

## [0.5.0] - 2024-01-19

### Changed

- improve progress reporting by reporting each conversion step separately

### Added

- graph_op `collapse` can collapse an edge component, i. e., it merges all nodes in a connected subgraph in said component
- `collapse` can be accelerated when all edges of the component to be collapsed are known to be disjoint by providing `disjoint = true` in the step config
- `collapse` provides more feedback on current process
- `collapse` gives hypernodes proper names that allow to identify the subgraph they belong to. Furthermore already existing hypernode ids are not reused (in case multiple collapse operations are run on a graph).
- `CorpusStorage` is now quiet
- importing `exmaralda` does now has more features
- `exmaralda` can be exported
- `xlsx` import creates part of-edges between tokens and document nodes
- all imports add PartOf edges from nodes to their respective document (lowest corpus node)

### Fixed

- `link` now considers all matching nodes for the same value, so the correct amount of edges is created
- `exmaralda` returns error when there is no time value for a timeline item
- fixed and simplified import of corpus node annotations
- `exmaralda` import's paths to linked media files are relative to the working directory
- `xlsx` importer now adds `PartOf` relations to the document nodes  

## [0.4.0] - 2023-11-13

### Added

- a separator for joining node values in `link` can be set with attribute `value_sep`
- spreadsheet imports can now be configured with a fallback token column for annotation names not mentioned in a column map, an empty string means map to timeline directly
- graph_op `check` can now be configured to not let the entire processing chain fail, when a test fails, by setting `policy = "warn"` (default is `fail`)
- metadata can be imported from spreadsheets alongside the linguistic data in the workbook, a data and a metadata spreadsheet name or number can now be specified for importing xlsx
- add heuristic for KWIC visualizer in graphml export
- `re` is now `revise`
- `revise` can modify components
- `path` as a import format now triggers the embedding of path names as nodes into the graph; this is supposed to help to represent configuration files for ANNIS
- import `path` adds an `annis::file` annotation
- import `path` adds part-of edges
- very basic implementation of a generic xml importer
- import opus sentence alignments
- graph op `enumerate` to enumerate nodes, i. e., add numeric annotations to results of one or multiple queries
- add importer for the format used by the [TreeTagger](https://www.cis.uni-muenchen.de/~schmid/tools/TreeTagger/)

### Fixed

- mapping annotations now correctly extracts the id of the node to apply a new annotation to
- linking nodes failed to extract node names when graphANNIS responded with a node name only (e. g. in case of "tok" or "node" in a query)
- linking nodes did not concatenate the values of multiple nodes properly, this is now fixed
- fixed code of spreadsheet import (merged cells might not have an end column reference)
- relative import and export paths are interpreted as relative to the parent directory of the workflow file
- the spreadsheet importer will use the correct namespace `default_ns` for segmentation ordering relations
- fixed ordering of token nodes in spreadsheet import

### Removed

- removed `show-documentation` subcommand and moved the documentation from mdBook to the crate documentation in the source code

## [0.3.1] - 2023-08-04

### Fixed

- Documentation was not included in release binaries.

## [0.3.0] - 2023-08-04

### Changed

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
- linker takes list of node indices for value nodes (source and target)
- return an error in Workflow::execute on conversion error instead of relying on status messages

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
- node linker: with two queries the resulting nodes can be linked via edges of a configurable type, layer, and name
- boolean environment variable `ANNATTO_IN_MEMORY` influences whether or not graphs will be stored on disk or in memory

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
