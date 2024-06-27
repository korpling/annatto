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

