# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

+ import EXMARaLDA files using Python
+ allow to leak graph updates to text file 
+ CoNLLImporter (Python)
+ graphupdate_utils for all python-based importers
+ properties are forwarded to python importers
+ basic finalizer for merge (merge is implicitly performed during multiple imports)
+ unified path handling in EXMARaLDA and CoNLL importer (still needs refactoring to util method)
+ added properties to CoNLLImporter to drop import of ordering and to qualify annotations with a separate name
+ simplified implementation of merge checker
+ merge checker always finishes and lists all misaligned documents
+ apply single combined update after imports are finished to avoid multiple calls to `apply_update`
+ externalized building of path/corpus structure for python modules to helper function
+ added property on.error to merge checker -- merge checker can now continue by dropping or forwarding erroneous documents if desired
