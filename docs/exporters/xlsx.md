# xlsx (exporter)

Exports Excel Spreadsheets where each line is a token, the other columns are
spans and merged cells can be used for spans that cover more than one token.

## Configuration

###  include_namespace

If `true`, include the annotation namespace in the column header.

###  annotation_order

Specify the order of the exported columns as array of annotation names.

Example:

```toml
[export.config]
annotation_order = ["tok", "lemma", "pos"]
```

Has no effect if the vector is empty.

