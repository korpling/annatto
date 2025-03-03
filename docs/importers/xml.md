# xml (importer)

Generic importer for XML files.

## Configuration

###  text_from_attribute

For specfic tag names, the covered text can be retrieved from
attribute values rather than the enclosed text. This is required
for unary tags, for example, especially for stand-off formats.
This attribute maps tag names to attribute names.

###  closing_default

The given string value will be appended to the covered text after
seeing the closing tag. A non-empty string is required to represent
unary tags. This is crucial for dealing with stand-off formats.

