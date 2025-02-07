# whisper (importer)

This module imports OpenAI's whisper json format.

Example:
```toml
[[import]]
format = "whisper"
path = "..."

[import.config]
skip_tokens = true
```

## Configuration

###  skip_tokens

With this attribute the tokenization in the output will not be imported,
instead the full text of each segment will serve as a token.

