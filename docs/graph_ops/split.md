# split (graph_operation)

This operation splits conflated annotation values into individual annotations.

## Configuration

###  delimiter

This is the delimiter between the parts of the conflated annotation in the input graph

###  anno

The annotation that holds the conflated values. Can be qualified with a namespace using `::` as delimiter.

###  layer_map

This maps a target annotation name to a list of potential values to be found in the split parts.

###  index_map

This maps annotation names that occur in a fixed position in the conflation sequence. This is easier especially for large numbers of annotation values.

###  delete

Whether or not to delete the original annotation.

