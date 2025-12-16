# xlsx (exporter)

Exports Excel Spreadsheets where each line is a token, the other columns are
spans and merged cells can be used for spans that cover more than one token.

## Configuration

###  include_namespace

If `true`, include the annotation namespace in the column header.

###  annotation_order

Specify the order of the exported columns as array of annotation keys.

Example:

```toml
[export.config]
annotation_order = ["tok", "lemma", "pos"]
```

Has no effect if the vector is empty.

###  skip_unchanged_files

If an output file for a document already exists and the content seems to
be the same, don't overwrite the output file.

Even with the same content, Excel files will appear as changed for
version control systems because the binary files will be different. When
this configuration value is set, the existing file will read and
compared to the file that will be generated before overwriting it.

###  update_datasheet

Set this to a sheet index or name to only update the data (not metadata)
in an existing workbook and not write a completely new file. If no target
file exists, a new workbook is created.

