# link (graph_operation)

Link nodes within a graph. Source and target of a link are determined via
queries; type, layer, and name of the link component can be configured.

This manipulator collects a source node set and a target node set given
the respective queries. In each node set, the nodes are mapped to a value.
Between each node in the source node set and the target node set, that are
assigned the same value, an edge is created (from source to target node).
The edges will be part of the defined component. Additionally, annotations
can be moved from the source or target node onto the edge.

The values assigned to each node in the source or target node set can be
created in several ways:
- a value from a single node in the query
- a concatenated value from multiple nodes in the query
- a concatenated value using a delimiter (`value_sep`) from multiple nodes in the query

The value formation is the crucial part of building correct edges.

Example:
```toml
[[graph_op]]
action = "link"

[graph_op.config]
source_query = "tok _=_ id @* doc"
source_node = 1
source_value = [3, 2]
target_query = "func _=_ norm _=_ norm_id @* doc"
target_node = 2
target_value = [4, 3]
target_to_edge = [1]
component = { ctype = "Pointing", layer = "", name = "align" }
value_sep = "-"
```

The example builds the source node set by trying to find all tok-nodes that have an id
and are linked to a node with a `doc` annotation (the document name) via a PartOf edge.
As source node, that goes into the said, the first (`1`) node from each result is
chosen, i. e. the token. The value, that is used to find a mapping partner from the
target node set, is build with the third (`3`) and second (`2`) node, concatenated
by a dash (s. `value_sep`). So a token with id "7", which is part of "document1",
will be assigned the value "document1-7".

The target configuration of query, node, and value maps nodes with a norm (`2`)
annotation to values, that concatenate the document name and the `norm_id`
annotation via a dash. So a norm token with id "7" in document "document1" will
also be assigned the value "document1-7".

This leads to edges from tokens with the same value to norm nodes with the same
value within the graphANNIS component `Pointing//align`.

Additionally, all edges are assigned a func annotation retrieved in the target query,
as `target_to_edge` is configured to copy annotation "1", which is `func` in the
example query, to the edge.


## Configuration

###  source_query

The AQL query to find all source node annotations. Source and target nodes are then paired by equal value for their query match.

###  source_node

The 1-based index selecting the value providing node in the AQL source query.

###  source_value

Contains one or multiple 1-based indexes, from which (in order of mentioning) the value for mapping source and target will be concatenated.

###  source_to_edge

This 1-based index list can be used to copy the given annotations from the source query to the edge that is to be created.

###  target_query

The AQL query to find all target node annotations.

###  target_node

The 1-based index selecting the value providing node in the AQL target query.

###  target_value

Contains one or multiple 1-based indexes, from which (in order of mentioning) the value for mapping source and target will be concatenated.

###  target_to_edge

This 1-based index list can be used to copy the given annotations from the target query to the edge that is to be created.

###  component

The edge component to be built.

###  value_sep

In case of multiple `source_values` or `target_values` this delimiter (default empty string) will be used for value concatenation.

