# Introduction


## Modules

Annatto comes with a number of modules, which have different types:

**Importer** modules allow importing files from different formats. 
More than one importer can be used in a workflow, but then the corpus data needs
to be merged using one of the merger manipulators.
When running a workflow, the importers are executed first and in parallel.


**Graph operation** modules change the imported corpus data. 
They are executed one after another (non-parallel) and in the order they have been defined in the workflow.

**Exporter** modules export the data into different formats.
More than one exporter can be used in a workflow.
When running a workflow, the exporters are executed last and in parallel.

## Creating a workflow file

Annatto workflow files list which importers, graph operations and exporters to execute.
We use an [TOML file](https://toml.io/) with the ending `.toml` to configure the workflow.
TOML files can be as simple as key-value pairs, like `config-key = "config-value"`. 
But they allow representing more complex structures, such as lists.
The [TOML website](https://toml.io/) has a great "Quick Tour" section which explains the basics concepts of TOML with examples.

### Import

An import step starts with the header `[[import]]`[^toml-array], and a
configuration value for the key `path` where to read the corpus from and the key `format` which declares in which format the corpus is encoded.
The file path is relative to the workflow file.
Importers also have an additional configuration header, that follows the `[[import]]` section and is marked with the `[import.config]` header.


```toml
[[import]]
path = "textgrid/exampleCorpus/"
format = "textgrid"

[import.config]
tier_groups = { tok = [ "pos", "lemma", "Inf-Struct" ] }
skip_timeline_generation = true
skip_audio = true
skip_time_annotations = true
audio_extension = "wav"
```

You can have more than one importer, and you can simply list all the different importers at the beginning of the workflow file.
An importer always needs to have a configuration header, even if it does not set any specific configuration option.

```toml
[[import]]
path = "a/mycorpus/"
format = "format-a"

[import.config]

[[import]]
path = "b/mycorpus/"
format = "format-b"

[import.config]

[[import]]
path = "c/mycorpus/"
format = "format-c"

[import.config]

# ...
```

### Graph operations

Graph operations use the header `[[graph_op]]` and the key `action` to describe which action to execute.
Since there are no files to import/export, they don't have a `path` configuration.

```toml
[[graph_op]]
action = "check"

[graph_op.config]
# Empty list of tests
tests = []
```

### Export

Exporters work similar to importers, but use the keyword `[[export]]` instead.

```toml
[[export]]
path = "output/exampleCorpus"
format = "graphml"

[export.config]
add_vis = "# no vis"
guess_vis = true
```

### Full example

You cannot mix import, graph operations and export headers. You have to first list all the import steps, then the graph operations and then the export steps.

```toml
[[import]]
path = "conll/ExampleCorpus"
format = "conllu"
config = {}

[[graph_op]]
action = "check"

[graph_op.config]
report = true

[[graph_op.config.tests]]
query = "tok"
expected = [ 1, inf ]
description = "There is at least one token."

[[graph_op.config.tests]]
query = "node ->dep node"
expected = [ 1, inf ]
description = "There is at least one dependency relation."

[[export]]
path = "grapml/"
format = "graphml"

[export.config]
add_vis = "# no vis"
guess_vis = true

```


[^toml-array]: TOML can represent lists of the things as [Arrays of Tables](https://toml.io/en/v1.0.0#array-of-tables).
