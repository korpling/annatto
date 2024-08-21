# conllu (exporter)

This module exports a graph in CoNLL-U format.

## Configuration

###  doc

This key is used to determine nodes that whose part-of subgraph constitutes a document, i. e. the entire input for a file.
Default is `annis::doc`, or `{ ns = "annis", name = "doc" }`.

Example:
```toml
[export.config]
doc = "annis::doc"
```

###  groupby

This optional annotation key is used to identify annotation spans, that constitute a sentence. Default is no export of sentence blocks.
Default is `annis::doc`, or `{ ns = "annis", name = "doc" }`.

Example:
```toml
[export.config]
groupby = "norm::sentence"
```

###  ordering

The nodes connected by this annotation component are used as nodes defining a line in a CoNLL-U file. Usually you want to use an ordering.
Default is `{ ctype = "Ordering", layer = "annis", name = "" }`.

Example:
```toml
[export.config]
ordering = { ctype = "Ordering", layer = "annis", name = "norm" }
```

###  form

This annotation key is used to write the form column.
Default is `{ ns = "annis", name = "tok" }`.

Example:
```toml
[export.config]
form = { ns = "norm", name = "norm" }
```

###  lemma

This annotation key is used to write the lemma column.
Default is `{ ns = "", name = "tok" }`.

Example:
```toml
[export.config]
lemma = { ns = "norm", name = "lemma" }
```

###  upos

This annotation key is used to write the upos column.
Default is `{ ns = "", name = "upos" }`.

Example:
```toml
[export.config]
upos = { ns = "norm", name = "pos" }
```

###  xpos

This annotation key is used to write the xpos column.
Default is `{ ns = "", name = "xpos" }`.

Example:
```toml
[export.config]
upos = { ns = "norm", name = "pos_spec" }
```

###  features

This list of annotation keys will be represented in the feature column.
Default is the empty list.

Example:
```toml
[export.config]
features = ["Animacy", "Tense", "VerbClass"]
```

###  dependency_component

The nodes connected by this annotation component are used to export dependencies.
Default is none, so nothing will be exported.

Example:
```toml
[export.config]
dependency_component = { ctype = "Pointing", layer = "", name = "dependencies" }
```

###  dependency_anno

This annotation key is used to write the dependency relation, which will be looked for on the dependency edges.
Default is none, so nothing will be exported.

Example:
```toml
[export.config]
dependency_anno = { ns = "", name = "deprel" }
```

###  enhanced_components

The listed components will be used to export enhanced dependencies. More than
one component can be listed.
Default is the empty list, so nothing will be exported.

Example:
```toml
[export.config]
enhanced_components = [{ ctype = "Pointing", layer = "", name = "dependencies" }]
```

###  enhanced_annos

This list of annotation keys defines the annotation keys, that correspond to the
edge labels in the component listed in `enhanced_components`. The i-th element of
one list belongs to the i-th element in the other list. Default is the empty list.

Example:
```toml
[export.config]
enhanced_annos = ["func"]
```

###  misc

This list of annotation keys will be represented in the misc column.
Default is the empty list.

Example:
```toml
[export.config]
misc = ["NoSpaceAfter", "Referent"]
```

