# Make sure you installed grcov via cargo first and that the llvm tools are available
# 
# cargo install grcov
# rustup component add llvm-tools-preview

# Set some environment variables needed by grcov
export RUSTFLAGS='-Cinstrument-coverage'
export CARGO_INCREMENTAL=0
# Because these build flags are different, there will
# be a conflict between the build files generated by 
# this script and other cargo processes. Thus, store
# the build artifacts into a different directory.
export CARGO_TARGET_DIR='target/coverage/'

# Create build folder for coverage. Remove some old coverage files.
# If we remove these files, there might be a mixup between previous runs
# and the reported coverage is incosistent
rm -f target/coverage/tests.lcov
rm -rf target/coverage/html
mkdir -p target/coverage/

# Run all tests
LLVM_PROFILE_FILE='cargo-test-%p-%m.profraw' cargo test --all-features

# Generate HTML report in target/debug/coverage/index.html
grcov . --binary-path ./target/coverage/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore 'target/*' --ignore '/*' --ignore '**/tests.rs' -o target/coverage/html/
# Also generate a lcov file for further processing
grcov . --binary-path ./target/coverage/debug/deps/ -s . -t lcov --branch --ignore-not-existing --ignore 'target/*' --ignore '/*' --ignore '**/tests.rs' -o target/coverage/tests.lcov

# Cleanup
find . -name '*.profraw' -delete
