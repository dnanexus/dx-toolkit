# Changelog for dx-toolkit releases

This change log spiritually adheres to [these guidelines](http://keepachangelog.com/en/0.3.0/).

Categories for each release: Added, Changed, Deprecated, Removed, Fixed, Security

## [210.0] - 2017-02-09 beta

### Fixed

* Fix dx-get untar issue with leading / 

## [209.0] - 2017-02-09 stable

### Added

* Support to build of upload agent on RHEL7
* Ability to build and get multi-region apps with custom system requirements

### Fixed

* Environment file so that `source environment` works properly on RHEL7 
* Modified `dx-docker` so that `--rm` passes through gracefully
* Modified `dx-docker` so that the `HOME` environment variable defaults to `/root`

## [208.1] - 2017-02-02 - stable

### Fixed

* bug where `dx download` incorrectly interpreted the overwrite option during argument parsing

## [208.0]

No significant changes.

## [207.0] - 2017-01-26

### Added

* Workflow handler to the Java bindings
* Checksum verification for file downloads in Java

### Fixed

* Bug resulting in transient errors on large downloads
* `dx-app-wizard` now correctly specifies `timeoutPolicy`
* `dx-docker` now handles default working directory and override properly


## [206.3] - 2017-01-19

### Added

* On Mac OS dx-toolkit now supports TLS 1.2 by activating a virtualenv

### Changed

* `dx-app-wizard` now defaults to Ubuntu 14.04 as opposed to 12.04
* Cosmetic improvements to CLI `describe` and `ls` subcommands

### Deprecated

* Perl and Ruby bindings are no longer supported and associated code has moved to the `dx-toolkit/contrib` directory
