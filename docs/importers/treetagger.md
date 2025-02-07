# treetagger (importer)

Importer for the file format used by the TreeTagger.

Example:
```toml
[[import]]
format = "treetagger"
path = "..."

[import.config]
column_names = ["tok", "custom_pos", "custom_lemma"]
```

This imports the second and third column of your treetagger files
as `custom_pos` and `custom_lemma`.

## Configuration

###  column_names

Provide annotation names for the columns of the files. If you want the first column to be `annis::tok`,
use "tok" without the namespace (default).

###  file_encoding

The encoding to use when for the input files. Defaults to UTF-8.

###  attribute_decoding

Options are `None` (default) and `Entities`.

