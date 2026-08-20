#!/bin/bash

# Stop the script if any command exits with a non-zero return code
set -e

# Run static code checks
cargo fmt --check
cargo clippy -- --deny warnings

# Execute tests and calculate the code coverage both as lcov and HTML report
rm -f target/llvm-cov/tests.lcov
mkdir -p target/llvm-cov/
cargo llvm-cov --no-cfg-coverage --all-features --ignore-filename-regex 'tests?\.rs' --lcov --output-path target/llvm-cov/tests.lcov

# Use diff-cover (https://github.com/Bachmann1234/diff_cover) and output code coverage compared to main branch
mkdir -p target/llvm-cov/html/
OUTPUT="$(diff-cover target/llvm-cov/tests.lcov --format html:target/llvm-cov/html/patch.html)"
echo "$OUTPUT"
if [ -z "${CI}" ]; then
    echo "HTML report available at $PWD/target/llvm-cov/html/patch.html"
fi

# Extract the code coverage percentage and exit with error code if threshold is not reached
PERC_REGEX='.*Coverage: ([0-9]+)(\.[0-9]*)?\%.*'
if [[ $OUTPUT =~ $PERC_REGEX ]]; then
    PERCENTAGE="$((${BASH_REMATCH[1]}))"
    if [[ $PERCENTAGE -lt 80 ]]
        then
            >&2 echo "Code coverage threshold not reached"
            exit 3
        fi
fi
exit 0
