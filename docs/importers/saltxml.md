# saltxml (importer)

Imports the SaltXML format used by Pepper (<https://corpus-tools.org/pepper/>).
SaltXML is an XMI serialization of the [Salt model](https://raw.githubusercontent.com/korpling/salt/master/gh-site/doc/salt_modelGuide.pdf).

## Configuration

###  missing_anno_ns_from_layer

If `true`, use the layer name as fallback for the namespace annotations
if none is given. This is consistent with how the ANNIS tree visualizer
handles annotations without any namespace. If `false`, use an empty
string as annotation namespace.

