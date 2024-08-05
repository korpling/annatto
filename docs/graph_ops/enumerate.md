# enumerate (graph_operation)

Adds a node label to all matched nodes for set of queries with the number of
the match as value.

## Configuration

###  queries

A list of queries to find the nodes that are to be enumerated.

###  target

The target node in the query that is assigned the numeric annotation. Holds for all queries. This is a 1-based index and counts by mention in the query.

###  label

The anno key of the numeric annotation that should be created.
Example:
```toml
[export.config]
label = { ns = "order", name = "i" }
```

You can also provide this as a string:
```toml
[export.config]
label = "order::i"
```

###  value

An optional 1-based index pointing to the annotation node in the query that holds a prefix value that will be added to the numeric annotation.

###  start

This can be used to offset the numeric values in the annotations.

