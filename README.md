# Experimental prototype of how a Pepper implementation in Rust could look like

This prototype is meant to research new Pepper implementations and not meant to be used by end-users.
It will use the graphANNIS data model and storage instead of Salt and uses Rust to implement the basic workflow framework.

Ideas what could be included are e.g.
- ✓ import in parallel by importers producing graph updates
- ✓ manipulate non-parallel
- ✓ call all exporters in parallel
- ✓ adding the GraphML importer and exporter from graphANNIS
- adding the relANNIS importer from graphANNIS
- allow to execute Python3.0 scripts as modules

## Building

You need Rust installed (e.g. by using <https://rustup.rs/>).
Because we also bundle an embedded Python instance, some additional steps are required

1. Install pyoxidizer by running once
```bash
cargo install pyoxidizer
```
2. Download  a Python distribution to the `pyembedded` folder by executing the following command once
```bash
pyoxidizer generate-python-embedding-artifacts pyembedded
```
3. Set the environment variables necessary to build the project with `cargo`
```nash
export PYO3_CONFIG_FILE=$(pwd)/pyembedded/pyo3-build-config-file.txt
```
   