# diff (graph_operation)

Compare to sub graphs, derive a patch from one towards the other,
and apply it.

## Configuration

###  by

Provide an annotation key that distinguishes relevant sub graphs to match
differences between. Default is `annis::doc`, which means that diffs are
annotated by comparing documents with the same name in different subgraphs.

###  source_parent

The node name of the common parent of all source parent nodes matching the `by` key.
If you are importing your source data for comparison from a directory "path/to/a",
the value to be set here is "a". If you are importing source and target of the diff
comparison in one import and the data is in different subfolders of the import directory,
you have to qualify the path a little further, e. g. "data/a", if you are importing
from directory "data".

###  source_component

Provide the source component along which the source sequence of comparison
is to be determined (usually an ordering).

###  source_key

This annotation key determines the values in the source sequence.

###  target_parent

The node name of the common parent of all target parent nodes matching the `by` key.
If you are importing your target data for comparison from a directory "path/to/b",
the value to be set here is "b". For more details see above.

###  target_component

Provide the target component along which the target sequence of comparison
is to be determined (usually an ordering).

###  target_key

This annotation key determines the values in the target sequence.

###  algorithm

Define the diff algorithm. Options are `lcs`, `myers`, and `patience` (default).

