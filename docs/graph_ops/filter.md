# filter (graph_operation)

This module acts as a positive filter, i. e., all nodes that do not match the query and are not real tokens
are deleted. In inverse mode, all matching nodes (except real tokens) get deleted. This only applies to nodes
that are of node type "node". Other node types will be ignored.

The following example configuration deletes all nodes that are annotated to be nouns and are not real tokens:
```toml
[[graph_op]]
action = "filter"

[graph_op.config]
query = "pos=/NOUN/"
inverse = true
```

## Configuration

###  query

The AQL query to use to identify all relevant nodes.

Example:
```toml
[graph_op.config]
query = "pos=/NOUN/"
```

###  inverse

If this is set to true, all matching nodes, that are not coverage terminals ("real tokens"), are deleted. If false (default),
the matching nodes and all real tokens are preserved, all other nodes are deleted.

Example:
```toml
[graph_op.config]
query = "pos=/NOUN/"
inverse = true
```

