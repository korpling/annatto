# enumerate (graph_operation)

Adds a node label to all matched nodes for set of queries with the number of
the match as value.

## Configuration

###  queries

A list of queries to find the nodes that are to be enumerated.

###  target

The target node in the query that is assigned the numeric annotation.
Holds for all queries. This is a 1-based index and counts by mention in the query.
E. g., for a query "tok _=_ pos _=_ lemma", a target "2" refers to the node holding
the `pos` annotation, "3" to the node holding the `lemma` annotation. The index picks
a node for EACH result of the query, that is returned, i. e. for the given example query
and a target index 2, each node with a `pos` annotation that overlaps identically with
a `tok` node and a node holding a lemma annotation, is selected as an annotation target.

###  by

First sort by the values of the provided node indices referring to the query. Sorting is stable. The first index ranks higher then the second, an so forth.
Everytime the value or the tuple of values of the selected nodes changes, the count is restartet at the `start` value.
Example:
```toml
[graph_op.config]
query = "tok _=_ pos=/NN/ @* doc"
by = [3]
```

The example sorts the results by the value of doc (the rest is kept stable).

###  label

The anno key of the numeric annotation that should be created.
Example:
```toml
[graph_op.config]
label = { ns = "order", name = "i" }
```

You can also provide this as a string:
```toml
[graph_op.config]
label = "order::i"
```

###  value

An optional 1-based index pointing to the annotation node in the query that holds a prefix value that will be added to the numeric annotation.

###  start

This can be used to offset the numeric values in the annotations.

