# EMBOSS Needle Example Asset Source

This is a sample asset source directory that provides the asset configurations in the `dxasset.json` file, to build an
asset bundle which is the union of a number of APT and pip packages. Applets that depend on Emboss's python libraries
like `Bio.Emboss.Applications.NeedleCommandline` and other binaries/libraries listed in `dxasset.json` can make use of
this asset bundle.

Run the following command from this directory to build the asset bundle `EMBOSSNeedleAsset`:

`dx build_asset`
