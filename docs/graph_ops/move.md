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
"target", "in", and "out".

###  copy

Setting this to `true` keeps the original annotation.
Default is `false`.

###  multi

In case that a node (only for directions `source` and `target`)
receives multiple annotations, this case needs to be dealt with.
Mode "naive" (default) ignores and potentially overwrites annotations
created earlier in the process. Providing a delimiter joins all applicable
values:
```toml
[graph_op.config]
multi = { delimiter = "," }
```

Instead of joining nodes, they can also be distributed across multiple
annotations on the same node. In this case, the namespace will be
used as an index. You thus lose control over the maximal index used,
but you can still retrieve annotations with the bare annotation
name (e. g. for deletion down the line):
```toml
[graph_op.config]
multi = "index"
```
Note that index mode leads to loss of the namespace for all annotations,
i. e., nodes, that only carry one value, will still have namespace "0"
for their annotation.


