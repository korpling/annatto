# link (graph_operation)

Link nodes within a graph. Source and target of a link are determined via
queries; type, layer, and name of the link component can be configured.

## Configuration

###  source_query

The AQL query to find all source node annotations. Source and target nodes are then paired by equal value for their query match.

###  source_node

The 1-based index selecting the value providing node in the AQL source query.

###  source_value

Contains one or multiple 1-based indexes, from which (in order of mentioning) the value for mapping source and target will be concatenated.

###  target_query

The AQL query to find all target node annotations.

###  target_node

The 1-based index selecting the value providing node in the AQL target query.

###  target_value

Contains one or multiple 1-based indexes, from which (in order of mentioning) the value for mapping source and target will be concatenated.

###  link_type

The edge component type of the links to be built.

###  link_layer

The layer of the edge component containing the links to be built.

###  link_name

The name of the edge component containing the links to be built.

###  value_sep

In case of multiple `source_values` or `target_values` this delimiter (default empty string) will be used for value concatenation.

