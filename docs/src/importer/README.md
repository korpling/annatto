# Importer

## CONLL-U

Format name: `conllu`

Import files in the [CONLL-U
format](https://universaldependencies.org/format.html) from the Universal
Dependencies project.


## EXMARaLDA

Format name: `exmaralda`

Import [EXMARaLDA partition
editor](https://exmaralda.org/en/partitur-editor-en/) (`.exb`) files.

## GraphML

Format name: `graphml`

Imports files in the [GraphML](http://graphml.graphdrawing.org/) file which have
to conform to the [graphANNIS data model].

## Meta

Format name: `meta`

Works similar to the Pepper configuration value
[`pepper.before.readMeta`](https://corpus-tools.org/pepper/generalCustomizationProperties.html)
and imports metadata property files for documents and corpora by using the file
name as path to the document.

## None

Format name: `none`

A special importer that imports nothing.

## PTB

Format name: `ptb`

Imports files in the Penn Treebank (bracket) format.

## TextGrid

Format name: `textgrid`

Imports [Praat TextGrid text file format](https://www.fon.hum.uva.nl/praat/manual/TextGrid_file_formats.html).

## Excel-like

Format name: `xlsx`

Imports Excel Spreadsheets where each line is a token, the other columns are
spans and merged cells can be used for spans that cover more than one token.