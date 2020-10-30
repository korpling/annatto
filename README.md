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
- add a legacy Java and Salt interface for existing Pepper modules
