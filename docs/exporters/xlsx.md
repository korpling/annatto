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

###  document_key

Set this to a sheet index or name to only update the data (not metadata)
in an existing workbook and not write a completely new file. If no target
file exists, a new workbook is created.

###  sheet_key

The lowest corpus nodes can be interpreted as providing sheet instead of file data.
This requires the sheet nodes to point to nodes in the PartOf component, that hold
the document key (directly or indirectly). If no document key node is found, the export
will fail.

