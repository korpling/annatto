# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

## [0.2.0-SNAPSHOT]

### Added

- import exmaralda
- fix character buffer in exmaralda import
