# table (importer)

Import CSV files with token and token annotations.

## Configuration

###  column_names

If not empty, skip the first row and use this list as the fully qualified annotation name for each column.

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

###  empty_line_group

If given, treat empty lines as separator for spans of token (e.g.
sentences). You need to configure the name of the annotation to create
(`anno`).
Example:
```toml
[import.config]
empty_line_group = {anno="csv::sent_id"}
```
The annotation value will be a sequential number.

Per default, a span is created, but you can change the `component` e.g. to a one of the type dominance.

```toml
[import.config]
empty_line_group = {anno = "csv::sentence, value="S", component = {ctype="Dominance", layer="syntax", name="cat"}}
```


