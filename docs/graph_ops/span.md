# span (graph_operation)

This module can query annotations and create spans across
all matching nodes for the same value, adjacency is optional.

## Configuration

###  query

The query for retrieving the relevant annotation values and
nodes for the spans to be created.

###  node

The node index (starting at 1) to pick the target node for the new span.
Note, that the new span will not directly point to the target node, but
will have edges of component `component` (s. below) to the covered tokens.

###  anno

The annotation key holding the values on the newly created spans.

###  value

The query indices for determining an annotation value, join via empty string if
more than one index is provided.

###  adjacent

By default only adjacent matches (in base ordering) will be covered by a new span.
If discontinuous spans are legal or useful in your model, you can set this to `false`.

###  component

The component for the spanning edges, by default `{ ctype = "Coverage", layer = "annis", name = ""}` (the default coverage component).

