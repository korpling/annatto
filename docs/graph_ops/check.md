# check (graph_operation)

Runs AQL queries on the corpus and checks for constraints on the result.
Can fail the workflow when one of the checks fail

## Configuration

###  tests

The tests to run on the current graph.

###  report

Optional level of report. No value means no printed report. Values are `list` or `verbose`.

###  policy

This policy if the process interrupts on a test failure (`fail`) or throws a warning (`warn`).

###  save

Provide a path to a file containing the test report. The verbosity is defined by the report attribute.

###  overwrite

If a path is provided to option `save`, the file is appended to by default. If you prefer to overwrite,
set this attribute to `true`.

