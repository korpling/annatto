# toolbox (importer)

Import annotations provided in the fieldlinguist's toolbox text format.

## Configuration

###  target

This attribute sets the annotation layer, that other annotations will point to.
This needs to be set to avoid an invalid model.

###  span

The annotation names named here are considered single-valued per line. Space values
are not considered delimiters, but part of the annotation value. Such annotations
rely on the existence of the target nodes, i. e. annotation lines without any other
non-spanning annotation in the block will be dropped.

