# bytealignedtoolbox (importer)

Import annotations provided in the fieldlinguist's toolbox text format.

## Configuration

###  globals

The annotation names are considered global spans, i. e., they will cover all following
annotations' tokens until a follow-up value is defined. Also, they are considered single-valued
per line.

###  span

The annotation names named here are considered single-valued per line. Space values
are not considered delimiters, but part of the annotation value. Such annotations
rely on the existence of the target nodes, i. e. annotation lines without any other
non-spanning annotation in the block will be dropped.

###  ignore

Lists the annotation markers to be ignored.

###  explicit_null

Null values are represented as `-` in toolbox. If you want those to remain explicit
annotations, set `explicit_null = true`.

