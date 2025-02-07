# chunk (graph_operation)

Add a span annotation for automatically generated chunks.

Uses the [text-splitter](https://crates.io/crates/text-splitter) crate which
uses sentence markers and the given maximum number of characters per chunk
to segment the text into chunks.

## Configuration

###  max_characters

Maximum chunk length.

###  anno_key

Annotation key used to annotate chunks with a value.

###  anno_value

Used annotation value.

###  segmentation

Optional segmentation name.

