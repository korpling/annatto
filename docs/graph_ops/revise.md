# revise (graph_operation)

Manipulate annotations, like deleting or renaming them. If you set up different types of
modifications, be aware that the graph is updated between them, so each modification is
applied to a different graph.

## Configuration

###  node_names

A map of nodes to rename, usually useful for corpus nodes. If the target name exists,
the operation will fail with an error. If the target name is empty, the node will be
deleted.

###  remove_nodes

a list of names of nodes to be removed

###  remove_match

Remove nodes that match a query result. The `query` defines the aql search query and
`remove` is a list of indices (starting at 1) that defines which nodes from the query are actually
the ones to be removed. Please remember that two query terms can actually be one underlying node,
depending on the graph you apply it to.
Example:
```toml
[[graph_op]]
action = "revise"

[[graph_op.config.remove_match]]
query = "cat > node"  # remove all structural nodes with a cat annotation that dominate other nodes
remove = [1]

[[graph_op.config.remove_match]]
query = "annis:doc"  # remove all document nodes (this divides the part-of component into two connected graphs)
remove = [1]

[[graph_op.config.remove_match]]
query = "pos=/PROPN/ _=_ norm"  # remove all proper nouns and their norm entry as well
remove = [1, 2]
```

To only delete the annotation and not the node, give the referenced node
as `node` and the annotation key to remove as `anno` parameter.

```toml
[[graph_op.config.remove_match]]
query = "pos=/PROPN/ _=_ norm"
remove = [{node=1, anno="pos"}]
```

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

