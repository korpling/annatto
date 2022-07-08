# Annatto

This software aims to test and convert data within the [RUEG](https://hu.berlin/rueg)
research group at Humboldt-Universität zu Berlin. Tests aim at 
continuouly evaluating the state of the [RUEG corpus data](https://zenodo.org/record/3236068)
to early identify issues regarding compatibility, consistency, and 
integrity to facilitate data handling with regard to annotation, releases
and integration. 

For efficiency annatto relies on the [graphannis representation](https://korpling.github.io/graphANNIS/docs/v2.2/data-model.html)
and already provides a basic set of data handling modules. The set of 
modules can be extended by the user through custom python scripts to 
adapt the data handling workflow(s) easily.

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
   
