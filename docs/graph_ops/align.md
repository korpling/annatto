# align (graph_operation)

Aligns nodes identified by queries with edges in the defined component.

## Configuration

###  groups

Define node groups that should be aligned. Neighbouring node groups in the
provided list are aligned, given common nodes can be identified. You can
define more than two node groups.

Example:

```toml
[[graph_op]]
action = "align"

[[graph_op.config.groups]]
query = "norm @* doc"
link = 1
groupby = 2

[[graph_op.config.groups]]
query = "tok!=/ / @* doc"
link = 1
groupby = 2
```

The example links nodes with a `norm` annotation. It groups them by document name.
The nodes are aligned with `tok` nodes, also grouped by document names, which need
to be identical to the first group's document names to have them aligned.


###  component

This defines the component within which the alignment edges are created. The default
value is `{ ctype = "Pointing", layer = "", name = "align" }`.

Example:

```toml
[graph_op.config]
component = { ctype = "Pointing", layer = "", name = "align" }
```

###  method

Select an alignment method. Currently only `ses` is supported, but in the future
other methods might be available. Therefore this does not need to, but can be set.

