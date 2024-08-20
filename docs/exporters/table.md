# table (exporter)

This module exports all ordered nodes and nodes connected by coverage edges of any name into a table.

## Configuration

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

###  delimiter

The provided character defines the column delimiter. The default value is tab.

Example:
```toml
[export.config]
delimiter = ";"
```

###  quote_char

The provided character will be used for quoting values. If nothing is provided, all columns will contain bare values. If a character is provided,
all values will be quoted.

Example:
```toml
[export.config]
quote_char = "\""
```

###  no_value

Provides the string sequence used for n/a. Default is the empty string.

Example:
```toml
[export.config]
no_value = "n/a"
```

###  ingoing

By listing annotation components, the ingoing edges of that component and their annotations
will be exported as well. Multiple ingoing edges will be separated by a ";". Each exported
node will be checked for ingoing edges in the respective components.

Example:
```toml
[export.config]
ingoing = [{ ctype = "Pointing", layer = "", ns = "dep"}]
```

###  outgoing

By listing annotation components, the ingoing edges of that component and their annotations
will be exported as well. Multiple outgoing edges will be separated by a ";". Each exported
node will be checked for outgoing edges in the respective components.

Example:
```toml
[export.config]
outgoing = [{ ctype = "Pointing", layer = "", ns = "reference"}]
```

