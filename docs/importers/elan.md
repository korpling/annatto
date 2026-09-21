# elan (importer)

This importer reads ELAN files.

## Configuration

###  segmentations

The listed annotation names will be treated as segmentations (in the graphANNIS sense)
and be equipped with an ordering `Ordering/default_ns/{tier_name}`. A segmentation in
this sense would in other contexts be called a "tokenization". Sentence spans, on the
other hand, are usually not segmentations in the graphANNIS sense, unless you strictly
need them to be.

If your annotation names contain spaces, replace these with "_".

###  skip_time

Setting this to `true` suppresses the creation of time annotations. In the default case,
these are created when the time unit in the ELAN file is milliseconds.

