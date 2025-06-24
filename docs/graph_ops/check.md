# check (graph_operation)

Runs AQL queries on the corpus and checks for constraints on the result.
Can fail the workflow when one of the checks fail.

There are several ways to configure tests. The default test type is defined
by a query, that is run on the current corpus graph, an expected result, and
a description to ensure meaningful output. E. g.:

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/ _=_ pos=/VERB/"
expected = 0
description = "No stone is labeled as a verb."
```

The expected value can be given in one of the following ways:

+ exact numeric value (s. example above)
+ closed numeric interval

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/"
expected = [0, 20]
description = "The lemma stone occurs at most and 20 times in the corpus"
```

+ numeric interval with an open right boundary:

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/"
expected = [1, inf]
description = "The lemma stone occurs at least once."
```

+ a query that should have the same amount of results:

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/"
expected = "pos=/NOUN/"
description = "There are as many lemmas `stone` as there are nouns."
```

+ an interval defined by numbers and/or queries:

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/"
expected = [1, "pos=/NOUN/"]
description = "There is at least one mention of a stone, but not more than there are nouns."
```

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/"
expected = ["sentence", inf]
description = "There are at least as many lemmas `stone` as there are sentences."
```

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/"
expected = ["sentence", "tok"]
description = "There are at least as many lemmas `stone` as there are sentences, at most as there are tokens."
```

+ or a query on a corpus loaded from an external GraphANNIS data base:

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/"
expected = ["~/.annis/v4", "SameCorpus_vOlderVersion", "lemma=/stone/"]
description = "The frequency of lemma `stone` is stable between the current graph and the previous version."
```

There is also a second test type, that can be used to check closed class annotation layers' annotation values:

```toml
[[graph_op.config.tests]]
[graph_op.config.tests.layers]
number = ["sg", "pl"]
person = ["1", "2", "3"]
voice = ["active", "passive"]
```

For each defined layer two tests are derived, an existence test of the annotation
layer and a test, that no other value has been used. So the entry for `number`
above is equivalent to the following tests, that are derived internally:

```toml
[[graph_op.config.tests]]
query = "number"
expected = [1, inf]
description = "Layer `number` exists."

[[graph_op.config.tests]]
query = "number!=/sg|pl/"
expected = 0
description = "Check layer `number` for invalid values."
```

A layer test can also be applied to edge annotations. Assume there are
pointing relations in the tested corpus for annotating reference and
an edge annotation `ref_type` can take values "a" and "k". The edge
name is `ref`. If in GraphANNIS you want to query such relations, one
would use a query such as `node ->ref[ref_type="k"] node`. For testing
`ref_type` with a layer test, you would use a configuration like this:

```toml
[[graph_op.config.tests]]
edge = "->ref"
[graph.config.tests.layers]
ref_type = ["a", "k"]
```


## Configuration

###  tests

The tests to run on the current graph.

###  report

Optional level of report. No value means no printed report. Values are `list` or `verbose`.

###  policy

This policy if the process interrupts on a test failure (`fail`) or throws a warning (`warn`).

###  save

Provide a path to a file containing the test report. The verbosity is defined by the report attribute.

###  overwrite

If a path is provided to option `save`, the file is appended to by default. If you prefer to overwrite,
set this attribute to `true`.

