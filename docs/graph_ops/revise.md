# revise (graph_operation)

Manipulate annotations, like deleting or renaming them.

## Configuration

###  node_names

A map of nodes to rename, usually useful for corpus nodes. If the target name exists,
the operation will fail with an error. If the target name is empty, the node will be
deleted.

###  remove_nodes

a list of names of nodes to be removed

###  move_node_annos

also move annotations to other host nodes determined by namespace

###  node_annos

rename node annotation

###  edge_annos

rename edge annotations

###  namespaces

rename or erase namespaces

###  components

rename or erase components. Specify a list of entries `from` and `to` keys, where the `to` key is optional
and can be dropped to remove the component.
Example:
```toml
[graph_op.config]
[[graph_op.config.components]]
from = { ctype = "Pointing", layer = "syntax", name = "dependencies" }
to = { ctype = "Dominance", layer = "syntax", name = "constituents" }

[[graph_op.config.components]]  # this component will be deleted
from = { ctype = "Ordering", layer = "annis", "custom" }
```

###  remove_subgraph

The given node names and all ingoing paths (incl. nodes) in PartOf/annis/ will be removed

