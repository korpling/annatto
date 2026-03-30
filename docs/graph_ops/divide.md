# divide (graph_operation)

This graph op can be used to split segment values into multiple sub nodes holding a character
or a predefined value.

Example:
```toml
source_anno = "norm::norm"
mode = "char"

[horizontal]
source = { ctype = "Ordering", layer = "default_ns", name = "norm" }
minimal = { ctype = "Ordering", layer = "annis", name = "" }
```

This splits value of "norm::norm" along the component of "Ordering/default_ns/norm" into characters.

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

Provide the vertical component type to build edges from old segments to new ones.
Default is "Coverage", but also different component type or a list of components can be provided.

###  source_anno

The annotation holding the value that is used for splitting into characters when mode "char" is used.

###  target_anno

The annotation holding the newly created value (depending on the chosen mode, see below).

###  mode

There are two modes, "char" splits values stored in the source key into characters, alternatively a dummy value
can be provided and the number of segments to be used.

Example:
```toml
target_anno = "annis::tok"
mode = { n = 3, value = " " }  # three tokens with an empty space per retrieved segment.
```

