# time (graph_operation)

This module adds time values to all nodes of type `node` in a graph. It either fills gaps in time values as long
as the start and end of an ordering have defined values, or it adds time values from 0 to the number of ordered
nodes in the case that absolutely no time values exist yet. In all other cases it will fail. Time values are
interpolated along ordering edges and propagated along coverage edges.

Example:
```toml
[[graph_op]]
action = "time"

[graph_op.config]
```

*No Configuration*
