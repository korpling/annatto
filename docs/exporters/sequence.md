# sequence (exporter)

This exports a node sequence as horizontal or vertical text.

## Configuration

###  horizontal

Choose horizontal mode if you want one group (e. g. sentence) per line,
choose false if you prefer one element per line.
In the latter case groups will be seperated by empty lines.

###  fileby

The annotation key that determines which nodes in the graph bunble a document in the part of component.

###  groupby

The optional annotation key, that groups the sequence elements.

###  group_component_type

the group component type can be optionally provided to define which edges to follow
to find the nodes holding the groupby anno key. The default value is `Coverage`.

###  component_type

The type of the edge component that contains the sequences that you wish to export.
The default value is `ordering`.

###  component_layer

The layer of the edge component that contains the sequences that you wish to export.
The default value is `annis`.

###  component_name

The name of the edge component that contains the sequences that you wish to export.
The default value is the empty string.

###  anno

The annotation key that determines the values in the exported sequence (annis::tok by default).

