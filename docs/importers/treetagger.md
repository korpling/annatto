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

You can use namespaces in some or all of the columns. The default
namespace for the first column if "tok" is provided is "annis".
For the following columns the namespace defaults to "default_ns" if
nothing is provided. If the first column is not "tok" or "annis::tok", "default_ns"
will also be the namespace if none is specified.

Example:
```toml
[import.config]
column_names = ["tok", "norm::custom_pos", "norm::custom_lemma"]
```

## Configuration

###  column_names

Provide annotation names for the columns of the files. If you want the first column to be `annis::tok`,
you can use "tok" or "annis::tok". For all following columns, if you do not provide a namespace, "default_ns"
will be used automatically.

###  file_encoding

The encoding to use when for the input files. Defaults to UTF-8.

###  attribute_decoding

Whether or not attributes should be decoded as entities (true, default) or read as bare string (false).

