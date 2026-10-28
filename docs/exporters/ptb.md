# ptb (exporter)

This module exports tree structures in parentheses format as known from Penn Treebank.

## Configuration

###  component

This is used to set the syntax component. The default is `{ ctype = "Dominance", layer = "syntax", name = ""}`.

###  file_component

This component is used to split the corpus graph in separate files. Default is `{ ctype = "PartOf", layer = "annis", name = ""}`.

###  file_key

The given key identifies the nodes gathering all nodes of a document. Default is `annis::doc`.

###  nonterminal_key

This annotation key is used to derive the category of nonterminal nodes. Default is `syntax::cat`.

###  terminal_key

This annotation key is used to derive the category of terminal nodes. Default is `syntax::pos`.

###  text_key

This annotation key contains the text value of the leaves of the tree. Default is `annis::tok`.

###  terminal_component

This component, usually an ordering, is used to identify the linear sequence of leave nodes. Default is `{ ctype = "Ordering", layer = "annis", name = ""}`.

