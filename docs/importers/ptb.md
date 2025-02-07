# ptb (importer)

Importer the Penn Treebank Bracketed Text format (PTB)

## Configuration

###  edge_delimiter

Some variants encode a function of the node after the node category,
such as "NP-subj", which means this nominal phrase node has the function
of the subject. If "-" is provided as `edge_delimiter`, the node
will carry a category "NP", whereas the ingoing edge will have a
function label "subj".

