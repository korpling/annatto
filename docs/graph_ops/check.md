# check (graph_operation)

Runs AQL queries on the corpus and checks for constraints on the result.
Can fail the workflow when one of the checks fail.

There are general attributes to control this modules behaviour:

`policy`: Values are either `warn` or `fail`. The former will only output
a warning, while the latter will stop the conversion process after the
check module has completed all tests. The default policy is `fail`.

`report`: If set to `list`, the results will be printed to as a table, if
set to `verbose`, each failed test will be followed by a short appendix
listing all matches to help you debug your data. If nothing is set, no report
will be shown.

`failed_only`: If set to true, a report will only contain results of failed tests.

`save`: If you provide a file path (the file can exist already), the report
is additionally saved to disk.

`overwrite`: If set to `true`, an existing log file will be overwritten. If set
to `false`, an existing log file will be appended to. Default is `false`.

Example:

```toml
[[graph_op]]
action = "check"

[graph_op.config]
report = "list"
save = "report.log"
overwrite = false
policy = "warn"
```

There are several ways to configure tests. The default test type is defined
by a query, that is run on the current corpus graph, an expected result, and
a description to ensure meaningful output. E. g.:

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/ _=_ pos=/VERB/"
expected = 0
description = "No stone is labeled as a verb."
```
A test can be given its own failure policy. This only makes sense if your global
failure policy is `fail` and you do not want a specific test to cause a failure.
A `warn` will always outrank a fail, i. e. whenever the global policy is `warn`,
an individual test's policy `fail` will have no effect.

Example:

```toml
[[graph_op.config.tests]]
query = "lemma=/stone/ _=_ pos=/VERB/"
expected = 0
description = "No stone is labeled as a verb."
policy = "warn"
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

A layer test can be defined as optional, i. e. the existence check is
allowed to fail, but not the value check (unless the global policy is `warn`):

```toml
[[graph_op.config.tests]]
optional = true
[graph_op.config.tests.layers]
number = ["sg", "pl"]
person = ["1", "2", "3"]
voice = ["active", "passive"]
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

###  failed_only

By setting this to `true`, only results of failed tests will be listed in the report (only works if a report level is set).

###  policy

This policy if the process interrupts on a test failure (`fail`) or throws a warning (`warn`).

###  save

Provide a path to a file containing the test report. The verbosity is defined by the report attribute.

###  overwrite

If a path is provided to option `save`, the file is appended to by default. If you prefer to overwrite,
set this attribute to `true`.

