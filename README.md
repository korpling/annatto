# Annatto

This software aims to test and convert data within the [RUEG](https://hu.berlin/rueg)
research group at Humboldt-Universität zu Berlin. Tests aim at 
continuouly evaluating the state of the [RUEG corpus data](https://zenodo.org/record/3236068)
to early identify issues regarding compatibility, consistency, and 
integrity to facilitate data handling with regard to annotation, releases
and integration. 

For efficiency annatto relies on the [graphANNIS representation](https://korpling.github.io/graphANNIS/docs/v2.2/data-model.html)
and already provides a basic set of data handling modules.

## Performing a release

You need to have [`cargo-release`](https://crates.io/crates/cargo-release) and
and [`mdbook`](https://crates.io/crates/mdbook) installed to perform a release.
Execute the follwing `cargo` command once to install these dependencies.

```bash
cargo install cargo-release mdbook
```

To perform a release, switch to the main branch and execute:

```bash
cargo release --execute
```

This will also trigger a CI workflow to create release binaries on GitHub.

## Funding

Die Forschungsergebnisse dieser Veröffentlichung wurden gefördert durch die Deutsche Forschungsgemeinschaft (DFG) – SFB 1412, 416591334 sowie FOR 2537, 313607803, GZ LU 856/16-1.

This research was funded by the German Research Foundation (DFG, Deutsche Forschungsgemeinschaft) – SFB 1412, 416591334 and FOR 2537, 313607803, GZ LU 856/16-1.
