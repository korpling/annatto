# treetagger (exporter)

Exporter for the file format used by the TreeTagger.

## Configuration

###  column_names

Provide the token annotation names that should be exported as columns.
If you do not provide a namespace, "default_ns" will be used
automatically.

###  segmentation

If given, use this segmentation instead of the token as token column.

###  span_names

Use a strategy to determine the SGML tag names for spans.

Use the *name* of the first annotation (default):
```toml
[export.config]
span_names = { strategy = "first_anno_name"}
```

Use the *namespace* of the first annotation:
```toml
[export.config]
span_names = { strategy = "first_anno_namespace"}
```

Use a *fixed name* for all spans:
```toml
[export.config]
span_names = { strategy = "fixed", name = "mytagname"}
```

###  doc_anno

The provided annotation key defines which nodes within the part-of component define a document. All nodes holding said annotation
will be exported to a file with the name according to the annotation value. Therefore annotation values must not contain path
delimiters.

Example:
```toml
[export.config]
doc_anno = "my_namespace::document"
```

The default is `annis::doc`.

###  skip_meta

Don't output meta data header when set to `true`

###  skip_spans

Don't output SGML tags for span annotations when set to `true`

