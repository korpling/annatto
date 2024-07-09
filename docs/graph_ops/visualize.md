# visualize (graph_operation)

Output the currrent graph as SVG or DOT file for debugging it.

**Important:** You need to have the[GraphViz](https://graphviz.org/)
software installed to use this graph operation.

## Configuration

###  limit_tokens

Configure whether to limit the number of tokens visualized. If `true`,
only the first tokens and the nodes connected to these token are
included. The specific number can be configured with the parameter
`token_limit`.
**Per default, limiting the number of tokens is enabled**

```toml
[[graph_op]]
action = "visualize"

[graph_op.config]
limit_tokens = true
token_limit = 10
```

To include all token, use the value `false`.
```toml
[[graph_op]]
action = "visualize"

[graph_op.config]
limit_tokens = false
```

###  token_limit

If `limit_tokens` is set to `true`, the number of tokens to include.
Default is `10`.

###  root

Which root node should be used. Per default, this visualization only
includes the first document.

```toml
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

###  output_dot

If set, a DOT file is created at this path (relative to the workflow directory).
The default is to not create a DOT file.

###  output_svg

If set, a SVG file is created at this path, which must is relative to the workflow directory.
The default is to create a SVG file at the path `graph-visualization.svg`.

