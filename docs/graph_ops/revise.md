# revise (graph_operation)

Manipulate annotations, like deleting or renaming them. If you set up different types of
modifications, be aware that the graph is updated between them, so each modification is
applied to a different graph.

## Configuration

###  node_names

A map of nodes to rename, usually useful for corpus nodes. If the target name exists,
the operation will fail with an error. If the target name is empty, the node will be
deleted.

Example:
```toml
[[graph_op]]
action = "revise"

[graph_op.config.node_names]
"corpus-root" = "FancyCorpus"
"corpus-root/document1" = "FancyCorpus/document1"
"corpus-root/obsolete-document" = ""  # this one will be deleted
```

###  remove_nodes

a list of names of nodes to be removed.

Example:
```toml
[[graph_op]]
action = "revise"

[graph_op.config]
remove_nodes = ["corpus/doc#tok1", "corpus/doc#tok2"]
```

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

###  node_annos

Modify annotation keys that are found on nodes in the graph. Leaving out the target key of
the renaming procedure will lead to deletion of the key.

Example:
```toml
[[graph_op]]
action = "revise"

[[graph_op.config.node_annos]]
from = "annis::tok"
to = "default_ns::word"

[[graph_op.config.node_annos]]
from = "norm::universal_pos"
to = "norm::upos"

[[graph_op.config.node_annos]]
from = "norm::comment"  # this annotation will be deleted, as there is no target key

```

###  edge_annos

Modify annotation keys that are found on edges of any edge component.
The mapping is configured analogous to `node_annos` (see above).

###  namespaces

Rename or erase namespaces (=rename with empty string).

Example:
```toml
[[graph_op]]
action = "revise"

[graph_op.config.namespaces]
"norm" = "default_ns"
"" = "default_ns"  # every empty namespace in an annotation key will be changed to "default_ns"
"dipl" = ""  # the namespace "dipl" will be replaced with the empty namespace
```

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

The given node names and all ingoing paths (incl. nodes) in PartOf/annis/ will be removed.

Example:

Imagine a corpus that consists of two documents, i. e., there is a root node called `corpus`
and two child nodes `corpus/doc_1` and `corpus/doc_2`, one for each document. Each document
then has many annotation nodes, such as tokens, as their children.

The following configuration deletes document `doc_2` and all its structural children:
```toml
[[graph_op]]
action = "revise"

[graph_op.config]
remove_subgraph = ["corpus/doc_2"]
```

Note that you have to mention the nodes' actual names, which in most cases, but not necessarily always, is
a path as in the example. But the underlying model allows to deviate from paths as node names.

