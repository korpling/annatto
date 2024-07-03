#!/bin/bash

cargo run -- documentation docs/
cargo about generate about.hbs -o third-party-licenses.html
