# conllu (importer)

Import files in the [CONLL-U format](https://universaldependencies.org/format.html)
from the Universal Dependencies project.

## Configuration

###  comment_anno

This key defines the annotation name and namespace for sentence comments, sometimes referred to as metadata in the CoNLL-X universe.
Example:
```toml
comment_anno = { ns = "comment_namespace", name = "comment_name"}

The field defaults to `{ ns = "conll", name = "comment" }`.
```

