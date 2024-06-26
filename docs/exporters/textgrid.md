# textgrid (exporter)

This exports annotation graphs to PRAAT TextGrids. Use is as follows:
```toml
[[export]]
format = "textgrid"
path = "your/target/path"

[export.config]
file_key = { ns = "my_namespace", name = "my_file_name_anno_name" }
time_key = { ns = "another_namespace", name = "the_name_of_time_values" }
point_tiers = [ { ns = "phonetic", "name" = "boundary_tone" } ]
remove_ns = true

```

## Configuration

###  file_key

This anno key determines which nodes in the part of subgraph bundle all contents for a file.
Example:
```toml
[export.config]
file_key = { ns = "annis", name = "doc" }  # this is the default and can be omitted
``````

###  time_key

This anno key is used to determine the time values.
Example:
```toml
[export.config]
time_key = { ns = "annis", key = "time" }  # this is the default and can be omitted
```

###  point_tiers

The annotation keys provided here will be exported as point tiers. The ones that are not mentioned will be exported as interval tiers.
Example:
```toml
[export.config]
point_tiers = [
{ns = "phonetics", name = "pitch_accent"},
{ns = "phonetics", name = "boundary_tone"}
]
```

###  remove_ns

This attribute configures whether or not to keep the namespace in tier names. If `true`, the namespace will not be exported.
Only set this to `true` if you know that an unqualified annotation name is not used for more than one annotation layer.
If used incorrectly, more than one layer could be merged into a single tier.
Example:
```toml
[export.config]
remove_ns = true
```

###  tier_order

Use this attribute to provide a list of anno keys in the order that you would like them to appear in the textgrid file.
If you want this to be an explicit allow list, i. e. you do not want to export other names than the ones in this list,
additionally set `ignore_others` to `true`.
Example:
```toml
[export.config]
tier_order = [
{ ns = "", name = "norm" },
{ ns = "norm", name = "pos" }
{ ns = "norm", name = "lemma" }
]
ignore_others = true
```

###  ignore_others

Set this attribute to `true` to ignore all annotations whose key is not mentioned in attribute `tier_order` or `point_tiers`.
Example:
```toml
[export.config]
point_tiers = [ { ns = "phonetics", name = "boundary_tone" } ]
tier_order = [
{ ns = "", name = "norm" },
{ ns = "norm", name = "pos" }
{ ns = "norm", name = "lemma" }
]
ignore_others = true    ///
```

