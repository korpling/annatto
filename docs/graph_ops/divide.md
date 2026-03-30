# divide (graph_operation)



## Configuration

###  horizontal

This determines which component provides the set of nodes whose values require a smaller division
and in which component the divided nodes should be organized.
These are usually two orderings with the minimal being the default ordering "Ordering/annis". If
you want to use the default minimal you do not need to specify a value.

Example:
```toml
[[graph_op]]
action = "divide"

[graph_op.config.horizontal]
source = { ctype = "Ordering", layer = "default_ns", name = "norm" }
minimal = { ctype = "Ordering", layer = "annis", name = "" }
```

###  vertical

*No description*

###  source_anno

*No description*

###  target_anno

*No description*

###  op

*No description*

