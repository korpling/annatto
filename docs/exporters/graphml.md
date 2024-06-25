# graphml (exporter)

Exports files as [GraphML](http://graphml.graphdrawing.org/) files which
conform to the [graphANNIS data model](https://korpling.github.io/graphANNIS/docs/v2/data-model.html).

## Configuration

###  add_vis

If set, add this ANNIS visualization configuration string to the corpus
configuration. See
<http://korpling.github.io/ANNIS/4.11/user-guide/import-and-config/visualizations.html>
for a description of the possible visualization options of ANNIS.

###  guess_vis

Automatically generate visualization options for ANNIS based on the
structure of the annotations, e.g. `Dominance` edges are indicators that
a syntactic tree should be visualized.

###  stable_order

Always generate the same order of nodes and edges in the output file.
This is e.g. useful when comparing files in a versioning environment
like git.
**Attention: this is slower to generate.**

###  zip

Output a ZIP file that includes the GraphML file. Linked files (like
e.g. audio files) are included if they have been referenced by a
*relative* path. Since GraphML is easily compressed this can help with
storage size. It also improves the IMPORT in the ANNIS frontend, which
only accepts ZIP files.

