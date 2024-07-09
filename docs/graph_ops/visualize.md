# visualize (graph_operation)

Output the currrent graph as SVG for debugging it.

## Configuration

###  max_token_number

Limit number of token visualized. If given, only the first token and the
nodes connected to these token are included. The default value is `50`.

###  root

Which root node should be used. Per default, this visualization only
includes the first document.

``toml
[[graph_op]]

action = "visualize"

[graph_op.config]
root = "first_document"
```
Alternativly it can be configured to include all documents (`root = "all"`) or you can give the ID of the document as argument.
``toml
[graph_op.config]
root = "mycorpus/subcorpus1/mydocument"
```

