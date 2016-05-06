# [QIIME](http://qiime.org/) Example Asset Source

This is a sample asset source directory that provides the asset configurations in the `dxasset.json` and `Makefile`, to build an asset bundle which is a more complex example that installs and builds qiime dependencies. Applets that depend on qiime scripts like [validate_mapping_file.py](http://qiime.org/scripts/validate_mapping_file.html), [core_diversity_analyses.py](http://qiime.org/scripts/core_diversity_analyses.html), etc. can make use of this asset bundle.

Run the following command from this directory to build the asset bundle `qiime_microbiome_asset`:

`dx build_asset`
