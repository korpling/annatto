# sequence (exporter)

This exports a node sequence as horizontal or vertical text.

## Configuration

###  delimiter

This influences the way the output is shaped. The default value is '\n', that means each annotation value
for the configured annotation key will be in a new line. Setting this to a single whitespace (' ') will lead
to one line per group (see groupby configuration). Setting this to the empty string can be useful for corpora,
in which each token corresponds to a character.

###  fileby

The annotation key that determines which nodes in the graph bunble a document in the part of component.

###  groupby

The optional annotation key, that groups the sequence elements.

###  group_component_type

the group component type can be optionally provided to define which edges to follow
to find the nodes holding the groupby anno key. The default value is `Coverage`.

###  component

This configures the edge component that contains the sequences that you wish to export.
The default value ctype is `Ordering`, the default layer is `annis`, and the default
name is empty.
Example:
```toml
[export.config]
component = { ctype = "Pointing", layer = "", name = "coreference" }
```

###  anno

The annotation key that determines the values in the exported sequence (annis::tok by default).

