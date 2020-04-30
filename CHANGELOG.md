# Changelog for dx-toolkit releases

This change log spiritually adheres to [these guidelines](http://keepachangelog.com/en/0.3.0/).

Categories for each release: Added, Changed, Deprecated, Removed, Fixed, Security

## Unreleased

## [295.0] - beta

### Fixed

* `dx get` for applets with `python3` interpreter

### Changed

* Python 2.7 example applets to use Python 3
* Commit dxpy version during release

## [294.0] - 2020.04.30 stable

* No significant changes

## [293.0] - 2020.04.24 

### Added

* `dx-mount-all-inputs` for dxfuse 
* Sci Linux compilation option for upload agent
* Python 3 interpreter for `dx-app-wizard`

### Fixed

* dxR build
* `dx upload` retry for "SSL EOF" error
* Error out for dx run --clone with analysis and executable

## [292.0] - 2020.04.09

### Added

* runSpec.version dxapp.json key for 16.04 applets
* `dx build_asset` support for runSpecVersion key in dxasset.json

### Fixed

* Python documentation build
* dxpy tests in Python 3 

### Changed

* Bump jackson-databind from 2.9.10 to 2.9.10.1 

## [291.1] - 2020.03.10 

### Changed

* Bump jackson-databind from 2.9.10 to 2.9.10.1 
* Retry symlink file downloads with `aria2c`

### Fixed

* Python3 issue in dx app builder test

### Added

* Remote app tarball builder for xenial
* Allow disabling system exit on log client
* database class in dx find data 
* Python3 compatibility for exec environment scripts
* pip3 package manager for execDepends

### Removed

* Precise debian package build target

## [290.1] - 2019.11.21 stable

### Changed

* Run job as high priority if '--ssh' provided

### Fixed

* Project deletion warning if specifying file-id

### Added

* New instance types to `dx-app-wizard`

## [289.0] - 2019.10.09 

### Changed

* Upgrade jackson to 2.9.10

### Fixed

* Python 3 wrapper generation and tests

## [288.0] - 2019.10.01 

### Added

* `dx get` for database files
* v2 instance types in `dx-app-wizard`

## [287.0] - 2019.08.30 

### Fixed

* Generating ruby wrappers in python 3
* dx-app-wizard in python 3

## [286.1] - 2019.07.08 

### Changed

* Documentation links to https://documentation.dnanexus.com

## [285.1] - 2019.07.08 

### Fixed

* Remove non-ascii char from readme

## [285.0] - 2019.06.19 

### Added

* '--xattr-properties' argument for dx-upload-all-outputs to include fs metadata as k,v properties
* xattr dependency for Linux builds

### Changed

* Only require futures package for python 2.7
* Upgrade build dependencies for pip, setuptools, and wheel

## [284.0] - 2019.06.13 stable

### Added

* DXJava support for proxies 
* Approved tools CLI for `dx update project`

### Changed

* Upgrade jackson-databind and jackson-core to version 2.9.8
* Provide project ID for dx make_download_url unless in job workspace

### Fixed

* Enabling argcomplete for `dx` installed with debian package in worker environment

## [283.0] - 2019.05.13 

### Changed

* `dx upgrade` downloads the latest version from s3

## [282.0] - 2019.05.08

### Changed

* Reduce the number of API calls for `dx download`

### Fixed

*  `dx upload` error via proxy in Azure

## [281.0] - 2019.04.18 

### Added 

* support for passing HTTPContext in `DXJava` to the `execute()` operation

## [280.0] - 2019.04.18 

### Added

* `--instance-count` to `dx run` so that Spark cluster size can be defined at app start

### Changed

* `dx wait` behavior by adding exponential backoff and passing appropriate project
* Decreased libcurl timeout in C++ bindings from infinity to default 10 min
* Default Ubuntu release to 16.04 in `dx-app-wizard` 
* Link handling to better support JBORs

### Fixed

* Handling file arrays in batch job runner

## [279.0] - 2019.04.11

* no significant updates

## [278.0] - 2019.03.21 

### Added

* new `findDataObjects` inputs to DXJava
* project name resolution to `--project` flag for `dx run`
* smart reuse and SAML identity provider
* `dx list database <entity>` for DNAnexus Apollo
* `--ignore-reuse` and `--ignore-reuse-stage` working for `dx run <workflow>`

### Changed

* Upgrade `proot` to be compatible with kernel >= 4.8
* Skip symlinks test in isolated environment

## [277.0] - 2019.03.14

### Fixed

* Uploading binary data, such at compressed files, works in python3.
* python3 parsing subcommand help output

### Added

* Binary mode for opening dx:files.
* A `--unicode` flag for command line tools that need to handle unicode text files.
  For example: `dx cat --unicode file-xxxx`. This was added for `cat`, and `download`.

### Removed

* 32-bit build target for Ubuntu 14.04
* `gtable` code

## [276.0] - 2019.02.08

### Added

* `--phi` flag for `dx new project`

### Fixed

* Bug in downloading symlinks when using aria2c
* Max number of aria2c connections <= 16

## [275.0] - 2019.02.01

### Fixed

* argcomplete eval in the worker when sourcing `environment`

## [274.0] - 2019.02.01

### Fixed

* Preserve `httpsApp` field in dxapp.json when calling `dx get`
* The `--except [array:file variable]` option for `dx-download-all-inputs`

## [273.0] - 2019.01.24

### Fixed
* upload issue using api proxy in Python 3
* `--no-project` option in `dx-jobutil-parse-link`

## [272.0] - 2019.01.17

### Added
* A script for reconnecting to a jupyter notebook session

## [271.0] - 2019.01.10

### Added
* support for dx building a global workflow with apps in multiple regions

### Changed

* Mark Ubuntu 12.04 as deprecated in `Readme`

### Fixed

* setting instance types on global workflow stages
* fix test code for Spark 2.4.0 upgrade

## [271.2] - 2019.01.03

### Fixed

* symlink download with `aria2`

## [270.1] - 2018.12.07

No significant changes

## [269.2] - 2018.12.07

### Fixed

* Update MANIFEST.in to include python `Readme`

## [269.1] - 2018.12.07

### Fixed

* Writing to stdout for py2 in `dx cat`
* Python virtualenv path in traceability runner

## [269.0] - 2018.12.07

### Fixed

* Failing `build app` when suggestion is not a dxlink
* Handle keyboard interrupt and system exit gracefully
* Use absolute path to set argcomplete
* If a bash variable is long, print a warning, do not omit it
* Issue with backports deps on Windows

### Changed

* Make `dx-toolkit` python 2 and 3 compatible
* Update macOS gnureadline version
* Allow Windows to use certifi CA bundle
* Update bindings for Apollo API routes
* Update urllib import in `dx-docker`
* Update requests in `make doc` target

### Added

* Test that attempts to upload to a closed file
* First draft of an environment file for fish shell
* If available, use `aria2` rather than `wget` as it's much faster

### Removed

* Use of ubuntu 12.04 in `test_dxclient.py`
* Old asset building script
* Rare subcommands (`compile`, `compile_dxni`, `sh`)
* The `dx-run-app-locally` script

## [268.1] - 2018.11.29

### Changed

* `jackson-databind` and `jackson-core` upgrade

## [267.1] - 2018.10.31

### Changes

* Update cran repository

## [267.0] - 2018.10.18

### Added

* Add release/distro to example app `dxapp.json` files

## [266.1] - 2018.10.18

### Fixed

* Download links for `docker2aci`
* The test error: No root certificates specified for verification of other-side certificates

## [266.0] - 2018.10.18

### Added

* A convenience login script for a cloud workstation
* Add `parents` param to `clone()`
* Allow batch IDs as output folders for batch run

### Fixed

* Setting a handler for a global workflow
* Redirecting proxy printout to stderr
* `cat` argument too long in a bash script
* Ensure we can pop items from the main dictionary (python3)

### Changed

* Warn user before `dx rm -r` on root of a project
* Let `urllib3` set default CA bundle on Windows
* Updgrade `pyopenssl` for test suite to 17.5.0
* Replace `ws4py` with websocket-client library in `job_log_client`


## [265.0] - 2018.10.15

### Added

* Pass stages input from assetDepends to bundledDepends
* Traceability ID for locked workflow test (#406)

### Fixed

* Python 3 incompatibilites
* Python 3 `dx upload`
* `import dxpy` when `sys.stdin` is `None`

## [264.0] - 2018.09.06

### Fixed

* `dxpy.describe()` used with a list of object IDs and an additional `fields` argument

## [263.0] - 2018.08.30

### Fixed

* Sort inputs in `dx generate_batch_inputs`

## [262.0] - 2018.08.30

### Removed

* 12.04 builds

## [261.1] - 2018.08.24

### Changed

* Windows install Python version upgrade to 2.7.15

### Fixed

* Windows installation

## [261.0]

### Added

* `dx run --ignore-reuse` to disable job reuse for job
* `ignoreReuse` key in dxapp.json

## [260.0] - 2018.08.17

### Changed

* dxWDL version 0.74

## [259.0] - 2018.08.09

### Added

* Ubuntu release and asset version as parameters for `dx-docker create-asset`
* Builds for Ubuntu 16.04

### Fixed

* `dx wait` where a file with object IDs is provided in path
* `dx compile` for debian install dxWDL.jar filepath

## [258.1] - 2018.08.06

### Added

* Database wrappers
* Support dxni compilation

### Changed

* requests >= 2.8.0
* psutil >= 3.3.0

### Fixed

* Python 3 incompatibilities

## [257.3] - 2018.07.26

### Fixed

* Revert of finding project for data object

## [257.0]

### Added

* support for setting and updating `details` on global workflows
* decorators for traceability tests
* `dx watch` support for smart reuse
* test for `dx watch` job log relay
* `dx find jobs/executions` support for smart reuse
* ability to provide a file which contains a list of jobs or data objects upon which to `dx wait`
* `dxWDL` integration (`dx compile` command)

### Changed

* `python-dateutil` version upgrade to 2.5

### Fixed

* unclear behavior when `--clone`, `--folder`, and `--project` are simultaneously provided to `dx run`
* `DXObject.set_ids()` with project set to None (it is now set to an arbitrary project associated with the file)
* bash helpers tests

## [256.0]

### Changed

* Cosmetic update to `dx publish`
* `dx publish` now sets the `default` alias on the published version by default

## [255.0] - 2018.05.24

### Added

* Support for updating a global workflow
* Wiki links to API documentation for API wrappers
* addTags/removeTags API wrappers for global workflow

### Fixed

* Better checking for inputs and/or inputSpec before batch running workflows

## [254.0] - 2018.05.10

### Changed

* A workflow must be closed before initializing a global workflow from it (test updates for API changes)

## [253.0] - 2018.05.02

### Changed

* Remove the "temporary" field from `dx describe workflow-xxxx`

### Added

* "Deleted" field to `dx describe globalworkflow-xxxx`
* a note to `dx describe` that the workflow is locked

## [252.0] - 2018.04.26

### Added

* Print proper dx describe analysis of a global workflow
* `dx publish`   command
* inline cluster bootstrap script
* dx run for global workflows
* dx find apps tests
* resolve paths for global workflows
* add, remove, list users for global workflows
* add, remove, list developers for global workflows
* public project test
* API tests

## [251.2] - 2018.04.19

### Added

* Support for dx find global workflows
* Initial support for dx build global workflow
* Publish method to global workflow bindings
* Support for dx get global workflow

## [250.3] - 2018.04.12

### Fixed

* `dx download` for symlinks

## [250.0]

### Added

*  Support for `dx describe` for global workflows

## [249.0] -2018.04.05

### Added

* zsh support
* API wrappers for global workflow routes
* Basic Python bindings for global workflow
* `set_properties()` method to DXProject

### Fixed

* dx get applet without execDepends

## [248.0] - 2018.03.29

### Fixed

* `dx-clone-asset` SSL error

## [247.0] - 2018.03.14

### Added

* Clarify documentation of stage key for `dx run`
* Asset builder support for Ubuntu 16.04
* `--singlethread` parameter for `dx upload`

### Changed

* `dx-docker pull` retries more often

### Fixed

* c-ares package version for upload agent build
* Bug with Azure instance type selection in `dx-app-wizard`
* Do not retry code `422` in dxpy

## [246.0] - 2018.03.14

### Added

* `socketTimeout` and `connectionTimeout` parameters to JAVA `DXEnvironment` (thanks, @pkokoshnikov)
* Generate batch inputs CLI

### Changed

* Accept 302 as a success for `ua --test`

## [245.0] - 2018.02.15

### Added

* Batch runner CLI

### Changed

* Updated c-ares and file packages (related to upload agent builds)

## [244.0] - 2018.02.08

### Added

* API wrappers for `[applet,app,workflow]-xxxx/validateBatch`

### Fixed

* Issue where dx-clone-asset doesn't create projects in different regions if they don't already exist

## [243.0] - 2018.02.01

### Added

* App version and published/unpublished note to `dx run -h <app>`

### Changed

* Recursive search for asset in a path is disabled, and we strictly enforce only one matching asset name

## [242.0] - 2018.01.25

### Changed

* Use twine for pypy uploads

### Fixed

* Error that blocks org from being added as developers

## [241.0]

### Changed

* `dx-docker`: cleanup of the quote code (regression fix)
* `dx-docker`: use `shutil.move()` instead of `os.rename()` with the aci image
* Accept 'http_proxy', 'HTTP_PROXY', 'https_proxy', 'HTTPS_PROXY' for proxy environmental variables
* Error out instead of warning when old pricing scheme is used in `dxapp.json`
* Fix certain tests flakiness


## [240.1] - 2017.11.15

### Added

* Warning when `runSpec.release` is not specified in `dxapp.json` with a note it will be required in the future
* Numerous enhancements to Jupyter notebook support (see https://github.com/dnanexus/dx-toolkit/commit/7ecbcb6b75118c0acd27f7a7cfe37d0a19e6d6c3 for more information)

### Changed

* `dx-app-wizard` and `dx get` specify systemRequirements in `regionalOptions` and not in `runSpec` of `dxapp.json`
* multiple updates to jupyter notebook support

### Deprecated

* setting `systemRequirements` in `runSpec` of `dxapp.json`

### Removed

* `dx-configset-to-fasta` script

### Fixed

* `dx-clone-asset` sets the default regions to be all regions that user has access to and skips redundant cloning to current region
* `dx build` now works seamlessly across apps and applets

## [239.0] - 2017.11.02

### Changed

* Update run input help and describe messages for locked workflows
* Warn when old, top-level pricing policy scheme is used in dxapp.json

### Removed

* `dx-gtable-to-csv` and `dx-gtable-to-tsv` scripts
* `dx-workflow-to-applet` script
* `include_hidden_links` parameter from clone()

## [238.1] - 2017.10.27

### Fixed

* dx-toolkit and apt conflict with argcomplete

### Added

* dx-clone-asset script
* `dx-docker`: ignore user argument if given
* TLS documentation

### Changed

* app building tests now include `runSpec.release` and `runSpec.distribution` in app specs
* `dx-docker`: better handling of quotes

## [237.0] - 2017.10.19

### Added

* New InvalidTLSProtocol Exception raised when connection fails due to wrong TLS protocol.

### Changed

* Remove rstudio option for `dx notebook`

## [236.2] - 2017.10.4


### Fixed

* dx-toolkit and apt conflict with jq

### Added

* Azure instance types to the list of available types in `dx-app-wizard`
* ua -- test now displays system messages coming from the apiserver.

### Changed

* Update references to workflow `inputs` and `outputs` to keep them in sync with API changes

## [235.1] - 2017.09.28

* No significant changes

## [233.0] - 2017.09.07

### Added

* Priority arg to `build_asset`
* Pass region-specific pricing policy in dxapp.json

## [232.1] - 2017.08.31

### Fixed

* Execution of old workflows built without explicit IO

## [231.0] - 2017.08.24

### Added

* CLI support for workflow lockdown

### Removed

* Deprecated `dx-mount`

## [230.0] - 2017.08.17

### Added

* Initial 'dx notebook' commit
* Python bindings for workflow-level input and output
* Support for the 'downloadRestricted' flag

## [229.0] - 2017.08.10

### Changed

* Default to 14.04 release for all instance types in `dx-app-wizard`

## [228.0] - 2017-07-27

* No significant changes

## [227.1] - 2017-07-20

* Point release to fix release version issues

## [227.0] - 2017-07-13

### Fixed

*  dx-jobutil-new-job now properly supports instance type

### Changed

* Installation instructions now centralized on Github page
* Incrementally upgraded dependencies for Java bindings

### Added

* Helper script to check TLS 1.2 support
* A `region` parameter can now be passed to `dx find projects`, `dx find data`, `dx find org projects`

## [226.0] - 2017-06-29

* No significant changes

## [225.0] - 2017-06-22

### Added

* A `region` parameter can now be passed to `dxpy.find_data_objects()` and `dxpy.find_projects()`

### Fixed

* `dx-docker` now no longer bind mounts `$HOME` and `/tmp` into the guest as this is consistent with Docker

## [224.0] - 2017-06-08

### Fixed

* Python 3 compatibility with `dx-app-wizard`
* `dx get` does not redundantly inline description and developerNotes in dxapp.json any more

### Added

* Client support for partial folder deletion

## [223.0] - 2017-06-02

### Added

* Add methods `__next__()` and `next()` to DXFile to complete iteration interface (thanks to Eric Talevich!)

## [222.0] - 2017-05-25

### Fixed

* `--bill-to` option is utilized when building multi-region apps with `dx build`

## [221.0] - 2017-05-18

### Changed

* Mac install no longer uses virtualenv. Instead, we ask users to install desired version of Python

### Fixed

* dx-docker bug where environment variables passed in through the CLI do not get set within container

### Added

* `dx build` creates a workflow on the platform based on the workflow's local source directory

### Removed

* the deprecated `dx-build-app` and `dx-build-applet` scripts (that were replaced with `dx build`) were removed

## [220.0] - 2017-05-11

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

## [214.1] - 2017-03-23

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
