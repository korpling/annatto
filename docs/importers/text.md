# text (importer)

Importer for plain text files.

Example:
```toml
[[import]]
format = "text"
path = "..."

[import.config]
tokenizer = { strategy = "treetagger", language="fr" }
file_encoding = "UTF-8"
```

## Configuration

###  file_encoding

The encoding to use when for the input files. Defaults to UTF-8.

###  tokenizer

Which tokenizer implementation to use.
In general, this is configured with the name of the `strategy` and
additional configuration values specific to this strategy.

```toml
[import.config]
tokenizer = { strategy = "treetagger", language="fr" }
```

Currently, only the `treetagger` strategy is available. It imitates the
behavior of the `utf8-tokenize.perl` script from the
[TreeTagger](https://www.cis.uni-muenchen.de/~schmid/tools/TreeTagger/)
and can be configured to use a language specific configuration with the
additional `language` parameter.

The `language` field is the ISO 639-1 language code and the following languages have specific implementations:
- English (en),
- Romanian (ro),
- Italian (it),
- French (fr),
- Portuguese (pt),
- Galician (gl),
- Catalan (ca)

The default is a generic language configuration, which works well with German texts.

