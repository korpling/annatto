# exmaralda (exporter)

Export [EXMARaLDA partition editor](https://exmaralda.org/en/partitur-editor-en/)
(`.exb`) files.

Example:

```toml
[[export]]
format = "exmaralda"
path = "exb/MyCorpus"

[export.config]
copy_media = false
```

## Configuration

###  copy_media

If `true`, copy linked media files to the output location.

Example:

```toml
[export.config]
copy_media = true
```

###  doc_anno

Using this annotation key, the corpus nodes that define the entire subgraph relevant for a file are identified.
The value will then be split by path delimiters and only the last segment is used.
Example:

```toml
[export.config]
doc_anno = { ns = "annis", name = "node_name" }
```
This defaults to `{ ns = "annis", name = "doc" }`.

###  tier_order

If there is a desired order in which the annotations should be displayed in EXMARaLDA,
it can be set here by providing a list. Not specifying a namespace will not be interpreted
as empty namespace, but will group all annotation names with any namespace sharing the
provided name, together.
Example:

```toml
[export.config]
tier_order = ["norm::norm", "dipl::dipl", "annotator"]
```

