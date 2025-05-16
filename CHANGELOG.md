# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.31.0] - 2025-05-16

### Added

- import `treetagger` allows to configure annotation namespaces for all columns
- adding `--save <path>` as annatto command saves the workflow to the given path with resolved runtime variables and a footer

### Changed

- `check` appends its output to existing files instead of overwriting the contents when `save` is used to store the test log
- import `treetagger` allows to use "annis::tok" for the first column as well
- import `treetagger`: `attribute_decoding` values are now lowercase (`entities`, `none`)
- import `xlsx`: `column_map` has no default value anymore and needs to be set

## [0.30.0] - 2025-05-13

### Added

- graph_op `map`: mappings can now additionally be defined inline together with or instead of separate mapping file

### Changed

- graph_op `map`: rules are defined providing an anno key (via `anno`) like all other modules, `ns` and `name` will not work anymore

### Fixed

- graph_op `map`, import `webanno`: deny unknown fields in deserialization
- import `textgrid` goes not panic with wrong tier configuration, but provides a proper error message

## [0.29.3] - 2025-05-05

### Fixed

- import `xlsx`: removed multi-threading feature

## [0.29.2] - 2025-05-05

### Fixed

- import `xlsx` reports progress again

## [0.29.1] - 2025-05-02

## [0.29.0] - 2025-05-02

### Changed

- import `xlsx` runs each document in one thread

## [0.28.0] - 2025-05-02

### Added

- Add import `webanno` tsv format

### Changed

- `time` graph op does not by default use a fallback start time, it has to be set using `fallback_start`
- `check` outputs expected and actual result (in that order) whenever a test fails
- updates from manipulators and importers are now faster, as graph statistics are only computed on demand.

## [0.27.1] - 2025-03-25

### Fixed

- import `xlsx` removes delimiter "::" from qualified annotation names when building node names for more robust data processing.

## [0.27.0] - 2025-03-21

### Added

- export `exmaralda` allows to define a tier order

### Changed

- `time` uses a default start

## [0.26.0] - 2025-03-21

### Changed

- update to graphANNIS 3.7

### Fixed

- export `exmaralda` always writes timeline items in correct order (chronologically)

## [0.25.0] - 2025-03-03

### Added

- enable import of `xml` to represent stand-off data

## [0.24.0] - 2025-02-07

### Added

- Add import `whisper` (json format)

### Fixed

- Fixed bug in computation of valid row indices in import of `xlsx`, which led skipping cells at the end of a document
- refactor the code base of import `xlsx` for easier future debugging

## [0.23.0] - 2025-01-30

### Added

- import of `xml` includes tag names as annotation names with their contained text as annotation value

## [0.22.0] - 2025-01-24

### Added

- `conllu` supports multi-tokens in import

## [0.21.0] - 2025-01-07

### Added

- `map` allows to delete annotation via query index
- `enumerate` via its attribute `by` allows to restart counting when the value of a node specified in the query changes.
- `sleep` lets annatto sleep

## [0.20.2] - 2024-12-18

### Fixed

- `textgrid` does not import empty timeline intervals anymore
- `conllu` now properly handles tokens that are not covered by the defined group span
- `conllu` export allows to use the same annotation key in multiple columns

## [0.20.1] - 2024-12-16

## [0.20.0] - 2024-12-15

### Changed

- import `table` uses different node name scheme (parallel to other import modules)
- `enumerate` now works slightly different internally
- `link` does not use a corpus storage anymore
- `link` allows to copy node annotations to the newly created edges

## [0.19.0] - 2024-11-20

### Added

- import `meta` additionally supports csv tables

### Changed

- import `exmaralda` allows tiers that have no speaker assigned
- import `meta` trims annotation keys and values for property format

### Fixed

- import meta csv tables iterates over all lines and completely imports the entire table unlike before
- import `conllu` did not fail on missing column, but rather dropped the remaining content without notification

## [0.18.0] - 2024-10-15

### Added

- Add new `table` importer for CSV files.
- Allow to set the order of output columns in `table` export with the parameter
  `column_names` and to skip the column header with the `skip_header` param. 
- `remove_match` option in `revise` now allows to delete the annotation but not
  the referenced node. 

### Fixed

- Export token in `table` exporter instead of ignoring them. You can disable
  exporting the token with the `skip_token` parameter.
- Fixed `find_connected` calls with `Bound::Included(usize::MAX)`, which can
  lead to invalid results when using the linear graph storage. Replaced with the
  correct `Bound::Unbounded`. 

## [0.17.0] - 2024-09-27

### Added

- Add `meta` as export format

### Fixed

- `xlsx` importer did not give the correct node name to segmentation token. Due
  to this inconsistency, span annotations on segmentation nodes where not
  connected to the segmentation token.
- unknown keys in toml configurations are now denied not only in config context, but globally in a workflow file

## [0.16.0] - 2024-09-02

### Added

- `table` export has feature to customize n/a-value, which by default is the empty string
- Add `conllu` as export format
- import of `conllu` now supports enhanced dependencies
- Adds `saltxml` export format
- Adds `time` graph op to add or enrich time annotations
- The `table` exporter now supports the `id_column` parameter to
  enable/disable the ID column.
- Importers that map directories to (sub)-corpora and files to documents can now also importt the
  corpus if the `path` argument points to a single file.
- `xlsx` importer now maps columns as spans if the column is not configured to
  be a `token_column`.

### Changed

- `exmaralda` import now ranks order of tlis higher than sorting by time value (more compatible with
  modern EXMARaLDA files)
- `xlsx` importer will connect spans to their corresponding segmentation node
  with coverage edges instead of connecting them with the base tokens generated
  for the timeline items. Thus, the configured connection between spans and base
  text is not lost.

### Fixed

- `exmaralda` import keeps events with missing time values

## [0.15.0] - 2024-08-14

### Added

- New command line argument `--in-memory` that has the same meaning as setting
  `ANNATTO_IN_MEMORY` to true but is easier to discover.
- `map` manipulator can now add annotated spans and copy values from existing
  annotations. The copied values can be manipulated using regular expressions and
  replacement values.
- Adds `saltxml` import format
- Adds `table` export format
- Adds `filter` graph op
- `table` export can include in- and outgoing edges

### Fixed

- Using the same type of manipulator in a workflow now shows the correct
  progress.
- Make the progress report in `revise` mode indeterminate because it is unclear
  how many operations are actually performed and finishing some steps instantly
  and others in minutes will cause extremely unrealistic time estimations.
- `table` export uses one additional criterion to identify timeline tokens: no outgoing coverage tokens (apart from being a member of Ordering/annis/)

## [0.14.0] - 2024-07-24

### Added

- `revise` now offers to delete nodes that match a query using list entry `[[remove_match]]` with keys `query` and `remove`.

### Changed

- internal changes in deserialization of annotation components and annotation keys (keys can be provided as string or in map notation), which changes the api and the way some workflow configurations are organised. Use `annatto info [module]` for more details. It does not affect behaviour once older workflows are adapted to the new interface.
- when an exmaralda file contains an empty url attributed for a referenced file, this does not raise a warning anymore, as this is the way exmaralda encodes it, when no media file is used.

### Fixed

- `textgrid` export creates intervals from global xmin to global xmax for all tiers

## [0.13.1] - 2024-07-11

### Fixed

- `textgrid` export did not generate intermediate empty intervals when xmax of an interval did not match xmin of the subsequent interval, which leads to hardly editable intervals in praat. This has been fixed.

## [0.13.0] - 2024-07-11

### Added

- `visualize` graph operation that allows to output the current graph (somehwere
  in the conversion process) to SVG or DOT for debugging.

### Fixed

- removed debug output

### Changed

- `collapse` uses deserializable component, thus attributes `ctype`, `layer`, and `name` are now under key `component`
- `collapse` only keeps annotations with namespace `annis` for nodes that were terminals in the collapsed components when transferring to the merged nodes to keep node status intact (e. g. token vs. not a token in terms of `annis::tok`).

## [0.12.0] - 2024-07-08

### Added

- `textgrid` export considers time annotations of covered nodes as well

### Fixed

- `textgrid` export can now handle `annis::time` intervals with an undefined right boundary (such intervals will be skipped)

### Changed

- `collapse` now also transfers annotations with namespace "annis" with the exception of "annis::node_name". This could lead to unstable results in case of conflicting values, such as for "annis::layer", but for most use cases this is not relevant yet. Not adding many of the before dropped annotations, though, was much more severe.

## [0.11.0] - 2024-07-05

### Added

- `textgrid` export now creates PRAAT TextGrid files from annotation graphs
- `textgrid` export can be configured for a desired order of tiers in the output files; the order of tiers can be incomplete, attribute `ignore_others` can be used to interprete the order as an allowlist
- `textgrid` export also looks into `point_tiers` `ignore_others` is on, since it is a reasonable expection the user could have. Thus, setting `ignore_others = true` with an empty `tier_order` would result in an export of all point tiers if at least one is set.
- `exmaralda` export can now be configured for the annotation key that provides a clue to which subgraph is relevant for a file

### Fixed

- code is more robust and more transparent to the user in case of unexpected data
- `textgrid` import now allows correct file type specification for short files

### Changed

- `revise` now deserialized components directly and uses different syntax. They are provided as a list of `from` and `to` component specifications.

## [0.10.0] - 2024-06-25

### Fixed

- preconfiguration of `arch_dependency` via `guess_vis` field of graphml export now only sets `node_key` mapping for named orderings. Setting it with an empty value did not address `annis::tok` contrary to what was expected to happen.
- some bare unwraps have been removed, thus exporting graphml is now more robust.

### Added

- New `annatto document <OUTPUT_DIR>` command that allows to generate markdown
  files with the module documentation in a given output directory. This command
  is executed in every pull request to keep the documentation up to date.
- `conllu` format now properly imports sentence comments, i. e. sentence level annotations that are not delimited by "=". This also requires such annotations to not contain a "=" at all. Such comments will be by default imported as values of `conll::comment` annotations.
  The annotation name can be adapted using attribute `comment_anno` of toml type `map` with keys `ns` and `name` (a serialization of graphANNIS' `AnnoKey`).

## [0.9.0] - 2024-06-24

### Added

- `link`, `map`, `enumerate`, and `collapse` have documentation visible to the user.

### Fixed

- documentation for import of `xlsx` showed wrong config doc string
- `link` does not use default `0` for `source_node` and `target_node` attributes anymore, since they are 1-based indices (instead, there is no default)

## [0.8.2] - 2024-06-21

### Fixed

- `sequence` export for horizontal data now also works in models with multiple segmentation and empty tokens
- `check` can now save without a panic when `report` attribute is omitted. `list` is the default report level which only applies to `save`, not to the `report` attribute itself, where the default is not to print.

## [0.8.1] - 2024-06-21

### Fixed

- `sequence` export for horizontal mode now works

## [0.8.0] - 2024-06-17

### Added

- Importer for the relANNIS format (<http://korpling.github.io/ANNIS/3.7/developer-guide/annisimportformat.html>)
- progress reports for `enumerate`, `link`, and `map`
- `revise` can now rename nodes using attribute `node_names`, e. g. for renaming (top level) corpus nodes. The syntax is equivalent to renaming annotations, thus renaming with an empty value will lead to deletion. Renaming with an existing value (also rename with self) will lead to an error.
- Add `zip` option to GraphML export to directly export as ZIP file which can be
  more easily imported in ANNIS.

### Changed

- update to dependencies to latest graphANNIS version

### Fixed

- Fix non-resolved relative path when importing EXMARaLDA files.
- Limit the table width when listing the module properties, so they fit in the
  current terminal.

## [0.7.0] - 2024-05-23

### Added

- `sequence` exports connected node's annotation values (e. g. ordered nodes) as vertical or horizontal sequences.
- `split` breaks up conflated annotation values into parts
- `revise` now offers to delete an entire subgraph from a node in the inverse direction of part of edges
- `enumerate` can prefix the numeric annotation it generates with an annotation value from the query match (use attribute `value` to point in the match list with a 1-based index)

### Changed

- `enumerate` uses u64 internally (to be in line with graphANNIS and to be deserializable)
- `collapse` now uses node ids that indicate the node names that entered the merge, the parent node is not indicated anymore
- `split` has default configuration/behaviour (do nothing); attribute `keep` is now `delete` to adhere to boolean default logic

### Fixed

- no more `annis::tok` labels for non-terminal coverage nodes in `xlsx` import
- hypernode id's are unified, in older versions it could happen that annotations get distributed about two or more hypernode instances due to invalid determination of the parent (part of-child)

## [0.6.0] - 2024-04-22

### Added

- Added simple chunker module based on
  [text-splitter](https://crates.io/crates/text-splitter).
- `check` can write check report to file
- `check` can test a corpus graph comparing results to an external corpus graph loaded from a graphANNIS database
- import `ptb` can now split node annotations to derive a label for the incoming edge, when a delimiter is provided
  using `edge_delimiter`. E. g., `NP-sbj` will create a node of category `NP`, whose incoming edge has function `sbj`,
  given the following config is used: `edge_delimiter = "-"`
- config attribute `stable_order` for exporting graphml enforces stable ordering of edges and nodes in output
- toml workflow files now strictly need to stick to known fields of module structs
- command line interface now has the `list` subcommand to list all modules and the `info`  subcommand to show the description and parameters of a module.o

### Changed

- The `check` module can now query the `AnnotationGraph` directly without using
  the `CorpusStorageManager`.
- `chunk` deserializes with empty config to default values

### Fixed

- Don't throw error if output directory for any workflow does not exist.
- import `ptb`: Also constituents get `PartOf` edges to their respective document node.

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
