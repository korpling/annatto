# move (graph_operation)

Annotation can be moved from edges of a component
to the source or target node, but also from nodes
to edges going out of or into the carrying node.

The following moves annotations from the edge to the
target node of the edge:
```toml
[graph_op.config]
component = { ctype = "Pointing", layer = "", name = "dep" }
anno = "default_ns::deprel"
direction = "target"
```

Moving to the target is the default and does not need
to be explicated.

This moves an annotation from nodes to ingoing edges:
```toml
[graph_op.config]
component = { ctype = "Pointing", layer = "", name = "dep" }
anno = "default_ns::pos"
direction = "in"
```

## Configuration

###  component

The annotation component the involved edges are
contained in.

###  anno

The annotation key of the annotation to be moved.

###  direction

The direction of move. Potential values are "source",
"target", "in", and "out". `source` and `target` imply
that annotations are retrieved from edges of the given
`component` and applied to the source or target node,
respectively. In this use case, you can specify what
should happen when multiple annotations are to be
applied to the same node via attribute `multi`.
There are currently three multi-value modes:

- `naive` (default): Each new annotation overwrites the last
one applied. This can be safely used when you either do not
care or know that no node is the target or source of more
than one edge.
- `index`: The namespace of the annotations will be replaced
with an index (starting at 0). The maximum index for a node
indicates how many annotations were applied to it. Searching
the annotations without a namespace later will safely return
values.
- `delim`: By providing `multi = { delim = "," }` all values will
be concatenated using the delimiter (a comma in this example).

Directions `in` and `out` search for annotations on nodes and apply
them to in/out-going edges of the given component.

Examples:

Move all dependency relation annotations from the edges to their
unique target nodes (therefore `multi` can be omitted and defaults
to `naive`):
```toml
[graph_op.config]
component = { ctype = "Pointing", layer = "", name = "dep" }
anno = "deprel"
direction = "target"
```

Move all "ref_type"-annotations from coreference
edges onto targets and delimit multiple values by "|":
```toml
[graph_op.config]
component = { ctype = "Pointing", layer = "", name = "coref" }
anno = "ref_type"
direction = "target"
multi = { delim = "|" }
```

###  copy

Setting this to `true` keeps the original annotation.
Default is `false`.

