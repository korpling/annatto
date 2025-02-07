# meta (importer)

Works similar to the Pepper configuration value
[`pepper.before.readMeta`](https://corpus-tools.org/pepper/generalCustomizationProperties.html)
and imports metadata property files for documents and corpora by using the file
name as path to the document.
Alternatively, you can import csv-tables, that specify the target node in a specific column. The
header of said column has to be provided as `identifier`, which also needs to be a used annotation
key found in the corpus graph at the target node.

Example (for csv files):
```toml
[import.config]
identifier = { ns = "annis", name = "doc" }  # this is the default and can be omitted
```

## Configuration

###  identifier

The annotation key identifying document nodes.

###  delimiter

The delimiter used in csv files.

