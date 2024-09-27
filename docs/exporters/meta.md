# meta (exporter)

This module exports annattos and peppers meta data format.
Generally all nodes are up for export as a single document,
thus the `name_key` is used to subset the nodes and define the node names.

Example (with default settings):
```toml
[[export]]
format = "meta"
path = "..."

[export.config]
name_key = "annis::doc"
only = []
write_ns = false
```

This is equivalent to:
```toml
[[export]]
format = "meta"
path = "..."

[export.config]
```

## Configuration

###  name_key

This key determines the value of the file name and which nodes are being exported into a single file,
i. e., only nodes that hold a value for the provided will be exported. If values are not unique, an
already written file will be overwritten.

Example:
```toml
[export.config]
name_key = "my_unique_file_name_key"
```

###  only

This option allows to restrict the exported annotation keys. Also, adding keys with namespace "annis"
here is allowed, as annotation keys having that namespace are ignored in the default setting.

Example:
```toml
[export.config]
only = ["annis::doc", "annis::node_name", "annis::node_type", "date"]
```

###  write_ns

By setting this to true, the namespaces will be exported as well. By default, this option is false.

Example:
```toml
[export.config]
write_ns = "true"
```
The namespace will be separated from the annotation name by `::`.

