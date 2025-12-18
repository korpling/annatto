# edit (graph_operation)

Use this to directly edit the graph via graph update instructions.

The following builds a graph and then reverts all steps back to the
empty graph:

```toml
[[graph_op]]
action = "edit"

[[graph_op.config.instructions]] # note that you can define more than one node to be added for the same type
do = "add"
nodes = ["a", "b"]
node_type = "corpus"

[[graph_op.config.instructions]] # note that theorectically you can define more than one node to be targeted by the annotation
do = "add"
nodes = ["a"]
anno = "annis::doc"
value = "a"

[[graph_op.config.instructions]]
do = "add"
nodes = ["b"]
anno = "annis::doc"
value = "b"

[[graph_op.config.instructions]]
do = "add"
nodes = ["a#t1", "a#t2", "b#t1", "b#t2"]

[[graph_op.config.instructions]]  # Note that you can define more than one edge for a single instruction, as long as the component is the same
do = "add"
edges = [
{ source = "a#t1", target = "a"},
{ source = "a#t2", target = "a"},
{ source = "b#t1", target = "b"},
{ source = "b#t2", target = "b"}
]
component = { ctype = "PartOf", layer = "annis", name = "" }

[[graph_op.config.instructions]]
do = "add"
edges = [{ source = "a#t2", target = "a#t1"}]
component = { ctype = "Pointing", layer = "", name = "dep" }

[[graph_op.config.instructions]]  # edge annotations also can target more than one edge
do = "add"
edges = [{ source = "a#t2", target = "a#t1"}]
component = { ctype = "Pointing", layer = "", name = "dep" }
anno = "default_ns::deprel"
value = "subj"

### now revert

[[graph_op.config.instructions]]
do = "rm"
edges = [{ source = "a#t2", target = "a#t1"}]
component = { ctype = "Pointing", layer = "", name = "dep" }
annos = ["default_ns::deprel"]

[[graph_op.config.instructions]]
do = "rm"
edges = [{ source = "a#t2", target = "a#t1"}]
component = { ctype = "Pointing", layer = "", name = "dep" }

[[graph_op.config.instructions]]
do = "rm"
edges = [
{ source = "a#t1", target = "a"},
{ source = "a#t2", target = "a"},
{ source = "b#t1", target = "b"},
{ source = "b#t2", target = "b"}
]
component = { ctype = "PartOf", layer = "annis", name = "" }

[[graph_op.config.instructions]]
do = "rm"
nodes = ["a#t1", "a#t2", "b#t1", "b#t2"]

[[graph_op.config.instructions]]
do = "rm"
nodes = ["a", "b"]
annos = ["annis::doc"]

[[graph_op.config.instructions]]
do = "rm"
nodes = ["a", "b"]
```

## Configuration

###  instructions

Provide a set of instructions.

