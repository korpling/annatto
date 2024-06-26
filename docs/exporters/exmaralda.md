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

