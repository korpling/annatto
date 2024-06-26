# collapse (graph_operation)

Collapse an edge component,

Given a component, this graph operation joins source and target node of each
edge to a single node. This could be done by keeping one of the nodes or by
creating a third one. Then all all edges, annotations, etc. are moved to the
node of choice, the other node(s) is/are deleted.

## Configuration

###  ctype

The component type within which to find the edges to collapse.

###  layer

The layer of the component within which to find the edges to collapse.

###  name

The name of the component within which to find the edges to collapse.

###  disjoint

If you know that any two edges in the defined component are always pairwise disjoint, set this attribute to true to save computation time.

