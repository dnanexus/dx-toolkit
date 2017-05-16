# Changelog for dx-toolkit releases

This change log spiritually adheres to [these guidelines](http://keepachangelog.com/en/0.3.0/).

Categories for each release: Added, Changed, Deprecated, Removed, Fixed, Security

## [Unreleased]

### Fixed

* `--bill-to` option is utilized when building multi-region apps with `dx build`

## [221.0] - beta

### Changed

* Mac install no longer uses virtualenv. Instead, we ask users to install desired version of Python

### Fixed

* dx-docker bug where environment variables passed in through the CLI do not get set within container

### Added

* `dx build` creates a workflow on the platform based on the workflow's local source directory

### Removed

* the deprecated `dx-build-app` and `dx-build-applet` scripts (that were replaced with `dx build`) were removed

## [220.0] - 2017-05-11 - stable

### Fixed

* Bug introduced in release 204.0: including app resources fails

### Added

* Ability to specify additional resources for multi-region apps

### Changed

* `dx ls -l` and friends now request only needed describe fields 

## [219.0] - 2017-04-28

### Added

* Ability to specify bundledDepends and assetDepends for multi-region apps

## [218.0]

### Changed

* Use DNAnexus DockerHub repo for dx-docker tests

### Fixed

* Issue where selecting from exactly 10 projects resulted in a stacktrace error

### Added

* `dx get workflow-xxxx` creates a local representation of a workflow

## [217.0] - 2017-04-13 

No significant changes. 

## [216.1] - 2017-04-06

### Fixed

* Python 3 compatibility with `repr` import

## [216.0]

No significant changes.

## [215.0]

### Added

* dx-docker test suite to toolkit
* Retry download of Docker image if there is transient network failure
* Allow image ID as a parameter for `dx-docker`; see wiki documentation for more details

## [214.1] - 2017-03-23 - stable

### Fixed

* missing pyasn1 module for OSX list of install dependencies; gets rid of import warning messages 

## [214.0]

### Added

* Alternative export for `dx-docker` when docker image is improperly exported by docker engine

### Fixed

* `dx run -i=filename` now prompts user to select input file if duplicate filenames exist in project
* `dx-docker create-asset` now supports output path
* `dx download` failure when run within project that user has lost access to
* `dx build -f` now removes all applets with matching name/directory

## [213.1] - 2017-03-16

### Fixed

* `dx-docker run` KeyError when docker image is built from container

## [213.0]

### Fixed

* Recursive file upload on Windows for the Upload Agent
* Show download progress for calls to `dx download -r`
* Issue where calls to `dxpy.download_all_inputs(parallel=True)` hang

## [212.0]

### Fixed

* Upload agent now does not gzip compress .gz files on Ubuntu 14.04
* Minor log message fix if file is already uploaded

### Added

* Mark routes as retryable for those that support idempotent calls (e.g. creating a new file)

### Removed

* High-level GTable bindings

## [211.0] - 2017-02-23

No significant changes.

## [210.0] - 2017-02-16 

### Fixed

* Fix `dx get` untar issue with leading /
* Missing `dx-verify-file` and `jq` dependencies on Windows

## [209.0] - 2017-02-09 

### Added

* Support to build of upload agent on RHEL7
* Ability to build and get multi-region apps with custom system requirements

### Fixed

* Environment file so that `source environment` works properly on RHEL7 
* Modified `dx-docker` so that `--rm` passes through gracefully
* Modified `dx-docker` so that the `HOME` environment variable defaults to `/root`

## [208.1] - 2017-02-02

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
