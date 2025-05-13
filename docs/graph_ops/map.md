# map (graph_operation)

Creates new or updates annotations based on existing annotation values.

The module is configured with TOML files that contains a list of mapping
`rules`. Each rule contains `query` field which describes the nodes the
annotation are added to. The `target` field defines which node of the query
the annotation should be added to. The annotation itself is defined by the
`ns` (namespace), `name` and `value` fields.

```toml
[[rules]]
query = "clean _o_ pos_lang=/(APPR)?ART/ _=_ lemma!=/[Dd](ie|as|er|ies)?/"
target = 1
anno = "indef"
value = ""
```

A `target` can also be a list. In this case, a new span is created that
covers the same token as the referenced nodes of the match.
```toml
[[rules]]
query = "tok=/more/ . tok"
target = [1,2]
anno = "mapper::form"
value = "comparison"
```

Instead of a fixed value, you can also use an existing annotation value
from the matched nodes copy the value.
```toml
[[rules]]
query = "tok=\"complicated\""
target = 1
anno = "newtok"
value = {copy = 1}
```

It is also possible to replace all occurences in the original value that
match a regular expression with a replacement value.
The `replacements` parameter is a list of pairs where the left part is the
search string and the right part the replacement string.
```toml
[[rules]]
query = "tok=\"complicated\""
target = 1
anno = "newtok"
value = {target = 1, replacements = [["cat", "dog"]]}
```
This would add a new annotation value "complidoged" to any token with the value "complicated".
You can define more

The `replacements` values can contain back references to the regular
expression (e.g. "${0}" for the whole match or "${1}" for the first match
group).
```toml
[[rules]]
query = "tok=\"New York\""
target = 1
anno = "abbr"
value = {target = 1, replacements = [["([A-Z])[a-z]+ ([A-Z])[a-z]+", "${1}${2}"]]}
```
This example would add an annotation with the value "NY".

The `copy` and `target` fields in the value description can also refer
to more than one copy of the query by using arrays instead of a single
number. In this case, the node values are concatenated using a space as
seperator.

You can also apply a set of rules repeatedly. The standard is to only
executed it once. But you can configure
```toml
repetition = {Fixed = {n = 3}}

[[rules]]
# ...
```
at the beginning to set the fixed number of repetitions (in this case `3`).
An even more advanced usage is to apply the changes until none of the
queries in the rules matches anymore.
```toml
repetition = "UntilUnchanged"

[[rules]]
# ...
```
Make sure that the updates in the rules actually change the condition of the
rule, otherwise you might get an endless loop and the workflow will never
finish!

If you want to delete an existing annotation while mapping, you can use `delete`, which accepts a list
of query node indices. This will not delete nodes, but the annotation described in the query. The given
example queries for annotations of name "norm", creates an annotation "normalisation" with the same value
at the same node and then deletes the "norm" annotation:

```toml
[[rules]]
query = "norm"
target = 1
anno = "normalisation"
value = { copy = 1 }
delete = [1]
```

## Configuration

###  rule_file

The path of the TOML file containing an array of mapping rules.
Use rule files when you want to apply a lot of rules to not blow
up the main configuration file.

###  mapping

This mechanism can be used to provide rules inline instead of in a
separate file. Also, both mechanisms can be combined.

Example:
```toml
[[graph_op]]
action = "map"

[graph_op.config.mapping]  # this part is optional and can be dropped for default values
repetition = "UntilUnchanged"

[[graph_op.config.mapping.rules]]
query = "norm"
target = 1
anno = "default_ns::normalisation"
value = { copy = 1 }
delete = [1]
```

###  debug

If you wish for detailled output about the match count of each rule,
set this to `true`. Default is `false`, so no output.

Example:
```toml
[[graph_op]]
action = "map"

[graph_op.config]
rule_file = "mapping-rules.toml"
debug = true
```

