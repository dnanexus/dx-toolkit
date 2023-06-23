# Changelog for dx-toolkit releases

This change log spiritually adheres to [these guidelines](http://keepachangelog.com/en/0.3.0/).

Categories for each release: Added, Changed, Deprecated, Removed, Fixed, Security

## Unreleased

## [351.0] - beta

* No significant changes

## [350.1] - 2023.6.23

### Added

* `dx watch` support for detailed job metrics (cpu, memory, network, disk io, etc every 60s)
* `--detailed-job-metrics` for `dx run`
* `--detailed-job-metrics-collect-default` for `dx update org`

## [349.1] - 2023.6.15

### Added

* `dx extract_assay`
* external_upload_restricted param for DXProject

## [348.0] - 2023.6.9

### Added

* dxpy dependencies test suite

### Changed

* Optimizations in Nextflow Pipeline Applet script to make fewer API calls when
concluding a subjob

## [347.0] - 2023.5.11

### Changed

* Bumped allowed `colorama` version to 0.4.6
* Allow `requests` version up to 2.28.x

### Removed

* Unneeded python `gnureadline` dependency
* Unused `rlcompleter` import which may break alternative readline implementations

## [346.0] - 2023.4.20

### Changed

* Help message of the `dx make_download_url` command

### Fixed

* Released Nextaur 1.6.6. It includes fixes to errorStrategy handling and an update to the way AWS instance types are selected based on resource requirements in Nextflow pipelines (V2 instances are now preferred)
* `ImportError` in test_dxpy.py
* Replaced obsolete built-in `file()` method with `open()`
* Printing HTTP error codes that were hidden for API requests to cloud storage

## [345.0] - 2023.4.13 

### Changed

* Bump allowed cryptography dxpy dependency version to 40.0.x
* Tab completion in interactive executions now works with `libedit` bundled in MacOS and does not require externally installed GNU `readline`
* Released Nextaur 1.6.5. It added a caching mechanism to `DxPath` file and folder resolution, which reduces number of DX API calls made during pipeline execution. It also fixes an occasional hanging of the headjob.

### Fixed

* Tab completion in interactive execution of `dx-app-wizard`
* `dx-app-wizard` script on Windows
* Tab completion in interactive executions on Windows

## [344.0] - 2023.4.2

### Changed

* Released Nextaur 1.6.4. It includes a fix to folder download, minor fixes and default headjob instance update (mem2_ssd1_v2_x4 for AWS, mem2_ssd1_x4 for Azure)
* Nextflow pipeline head job defaults to instance types mem2_ssd1_v2_x4 (AWS), azure:mem2_ssd1_x4 (Azure). No change to Nextflow task job instance types.

### Fixed

* Nextflow profiles runtime overriding fix

### Added

* Support for file (un)archival in DXJava
* `archivalStatus` field to DXFile describe in DXJava
* `archivalStatus` filtering support to DXSearch in DXJava
* `dx run` support for `--preserve-job-outputs` and `--preserve-job-outputs-folder` inputs
* `dx describe` for jobs and analyses outputs `Preserve Job Outputs Folder` field
* Record the dxpy version used for Nextflow build in applet's metadata and job log

## [343.0] - 2023.3.24 

### Changed

* Released Nextaur 1.6.3. It includes updates to wait times for file upload and closing, and a fix to default Nextflow config path
* Upgraded Nextflow to 22.10.7
* Nextflow assets from aws:eu-west-2

## [342.1] - 2023.3.8

### Added

* Pretty-printing additional fields for Granular Wait Times in `dx describe` for jobs and analyses

### Changed

* Released Nextaur 1.6.2. It includes bugfixes and default value of maxTransferAttempts used for file downloads is set to 3

### Fixed

* `dx find jobs` if stopppedRunning not in describe output

## [341.0] - 2023.3.3

### Added

* `dx ssh` to connect to job's public hostname if job is httpsApp enabled
* '--list-fields', '--list-entities', '--entities' arguments for `dx extract_dataset`

### Changed

* Released Nextaur 1.6.1. It includes an optimization of certain API calls and adds `docker pull` retry in Nextflow pipelines
* Increased dxpy HTTP timeout to 15 minutes
 
### Fixed

* Helpstring of '--verbose' arg

## [340.1] - 2023.2.25

### Changed

* Nextflow - updated default instance types based on destination region

### Fixed

* Use project ID for file-xxxx/describe API calls in dxjava DXFile
* Nextflow errorStrategy retry ends in 'failed' state if last retry fails

## [339.0] - 2023.2.10

* No significant changes

## [338.1] - 2023.1.27

### Added

* Support for Granular Spot wait times in `dx run` using `--max-tree-spot-wait-time` and `--max-job-spot-wait-time`
* Printing of Spot wait times in `dx describe` for jobs and workflows
* Support for private Docker images in Nextflow pipelines on subjob level

### Fixed

* Feature switch check for Nextflow pipeline build in an app execution environment
* `dx get database` command reads from the API server with the API proxy interceptor
* Regex global flags in path matching to support Py3.11
* `dx run --clone` for Nextflow jobs (clear cloned job's properties)
* Do not rewrite ubuntu repo mirror after failed execDepends install

### Changed

* Upgraded Nextflow plugin version to 1.5.0

## [337.0] - 2023.1.20

### Changed

* Upgraded Nextflow plugin version to 1.4.0
* Failed Nextflow subjobs with 'terminate' errorStrategy finish in 'failed' state
* Updated Nextflow last error message in case 'ignore' errorStrategy is applied.
* Exposed help messages for `dx build --nextflow`

## [336.0] - 2023.1.7

* No significant changes

## [335.0] - 2022.12.12

### Added

* Group name for developer options in Nextflow pipeline applet

### Fixed

* Printing too many environment values with debug set to true
* Preserving folder structure when publishing Nextflow output files
* Missing required inputs passed to `nextflow run`

## [334.0] - 2022.12.2

### Added

* `--external-upload-restricted` flag for `dx update project` and `dx find projects`
* Support for `--destination` in `nextflow build --repository`
* `resume` and `preserve_cache` input arguments to Nextflow applets to support Nextflow resume functionality
* Support for error handling with Nextflow's errorStrategy
* `region` argument to `DXProject.new()`

### Fixed

* retrieving session config when no parent process exists
* an issue with describing global workflows by adding a resources container as a hint for describing underlying workflows

## [333.0] - 2022.11.23

### Added

* `nextflow run` command in the log for easier debugging

### Fixed

* Overriding config arguments with an empty string for Nextflow pipelines

### Changed

* `psutil` version to 5.9.3 which includes wheelfiles for macOS arm64
* Set ignore reuse in the nextflow applet template
* Set `restartableEntryPoints` to "all" in the nextflow pipeline applet's `runsSpec`


## [332.0] - 2022.11.04

### Added

* A warning for `dx build` when app(let)'s name is set both in `--extra-args` and `--destination`

### Fixed

* An error when setting app(let)s name in `dx build` (now the name set via `--extra-args` properly overrides the one set via `--destination`)
*  `dx build --nextflow --repository` returns json instead of a simple string

### Changed

*  Help for building Nextflow pipelines is suppressed

## [331.0] - 2022.10.14

### Added

* Added: `dx find jobs --json` and `dx describe --verbose job-xxxx` with --verbose argument return field internetUsageIPs if the caller is an org admin and the org has jobInternetUsageMonitoring enabled
* Nextflow applets no longer have default arguments and required inputs

### Fixed

* `dx describe user-xxxx` will not try to print the name if it is not present in the API response
 
## [330.0] - 2022.10.4

### Added

* Initial support for Nextflow
* pyreadline3 dependency for Windows with Python >= 3.5

### Fixed 

* Do not check python3 syntax with python2 and vice versa in `dx build`
* `dx build` properly verifies the applet's name given in the `extra-args` parameter

## [329.0] - 2022.9.23

### Added

* `dx extract_dataset` command
* Optional pandas dependency for dxpy

### Changed
- `dxpy.find_one_project`, `dxpy.find_one_data_object`, `dxpy.find_one_app` raise `DXError` if `zero_ok` argument is not a `bool`

## [328.0] - 2022.9.8

### Added

* `--head-job-on-demand` argument for `dx run app(let)-xxxx` 
* `--head-job-on-demand` argument for `dx-jobutil-new-job`
* `--on-behalf-of <org>` argument for `dx new user`

### Changed 

* dx-toolkit never included in execDepends when building app(lets) with `dx build`

### Deprecated

* `--no-dx-toolkit-autodep` option for dx build

### Fixed

* Reduce the number of API calls for `dx run applet-xxxx` and `dx run workflow-xxxx`
* `dx upload f1 f2 --visibility hidden` now correctly marks both files as hidden
* `dx upload` retry on all types of SSL errors 

## [327.1] - 2022.8.12

### Fixed

* Parsing ignoreReuse in `dx build` of workflow

### Changed

* DXHTTPRequest to pass ssl_context

## [326.1] - 2022.7.7 

### Added

* '--rank' argument for `dx run`

### Fixed

* Do not use job's workspace container ID in /applet-xxxx/run for detached jobs

## [325.1] - 2022.5.25

### Fixed

* `dx describe` of executable with bundledDepends that is not an asset
* Building globalworkflow from existing workflow with `dx build --from`

## [324.1] - 2022.5.13

### Fixed

* Improvements to symlink downloading reliability by solely using `aria2c` and enhancing options around its use (removes `wget` option for downloading symlinked files, adds the ability to set max tries for aria2c, adds `-c` flag for continuing downloads, removes the `--check-certificate=false` option).
* `dx build` comparison of workflow directory to workflow name
* Set project argument for `dx run --detach` when executed from inside a job

### Changed

* Removed `wget` option for downloading symlinked files
* Bump allowed requests dxpy dependency version to 2.27.1

### Added

* New argument `symlink_max_tries` for `dxpy.download_dxfile()` with default value of 15

## [323.0] - 2022.4.28

### Changed

* Do not list folder contents to speed up `dx cd` 

## [322.1] - 2022.4.5

### Added

* API wrappers for `dbcluster`

### Fixed

* Pin websocket-client to 0.54.0 to fix `dx watch` output to include job output
* Do not install pyreadline on Windows with Python 3.10

## [321.0] - 2022.2.23

### Fixed

* KeyError in `dx-app-wizard --json`

### Changed

* dxjava dependencies log4j2, jackson-databind

## [320.0] - 2022.2.1 

### Fixed

* Python 3.10 collections imports
* Recursive folder download `dx download -r` of folders with matching prefix

## [319.2] - 2022.1.21 

### Fixed

* Incorrect setting of the `folder` input option when building global workflows
* Remove unused match_hostname urllib3 import 

### Added

* Support for qualified workflow & applet IDs and paths when using `dx build --from` with an applet/workflow
* Setting properties when building global workflows
* '--allow-ssh' parameter to `dx ssh`
* '--no-firewall-update' parameter to `dx ssh`

### Changed

* Detect client IP for SSH access to job instead of `*`

## [318.0] - 2022.1.6

### Fixed

* Python 3.10 MutableMapping import

### Added

* `--no-temp-build-project` for single region app builds.
* `--from` option to `dx build` for building a global workflow from a project-based workflow, including a workflow built using WDL

## [317.0] - 2021.12.8 

### Fixed

* Reduce file-xxxx/describe API load during `dx upload`
* `dx get` uses a region compatible with user's billTo when downloading resources

### Changed
 
* `dx run` warns users if priority is specified as low/normal when using '--watch/ssh/allow-ssh'

## [316.0] - 2021.11.17 

### Added

* Support for dxpy on macOS arm64
* Path input for `dx list database files`

### Fixed

* Python 3 SSH Host key output in `dx describe job-xxxx`

### Changed

* dxpy dependencies cryptography, websocket-client, colorama, requests

## [315.0] - 2021.10.28 

* No significant changes

## [314.0] - 2021.08.27 

### Added

* Support FIPS enabled Python
* `dx archive` and `dx unarchive` commands

### Fixed

* `dx upload` part retry where file would stay in an open state
* `dx run <globalworkflow> --project/--destination/--folder` now submits analysis to given project or path

## [313.0] - 2021.08.18 

### Added

* '--cost-limit' arg for `dx run` 
* '--database-ui-view-only' flag for `dx new project`

### Fixed

* `Total price` for `dx describe` prints formatted currency based on `currency` metadata

## [312.0] - 2021.07.06 

* No significant changes

## [311.0] - 2021.05.21 

### Added

* `DX_WATCH_PORT` env var for supporting `dx watch` in the job execution environment

## [310.0] - 2021.05.12 

* No significant changes

## [309.0] - 2021.04.28 

### Added

* `low` option for `--priority` argument for `dx run`

### Fixed

* Provide job container-id when downloading bundledDepends in job execution environment

### Changed

* Upgrade to proot 5.2 from udocker2 fork for `dx-docker`
* `dx-app-wizard` default to Ubuntu 20.04

## [308.0] - 2021.04.23 

### Fixed

* Search for reference genome project in region
* Connection leak with HttpClient in DXFile

## [307.0] - 2021.04.19 

### Added

* `--brief` flag to dx-clone-asset so that script results can be used downstream

### Changed

* Bump jackson-databind from 2.9.10.5 to 2.9.10.7

### Fixed

* xattr import in `dx-upload-all-outputs`

## [306.0] - 2021.01.21 

### Added

* Added '--keep-open' flag for `dx build`

### Fixed

* Symlink download retries when error 22 is thrown

## [305.0] - 2021.01.12 

### Added

* '--detach' flag for `dx run`

### Changed

* Add xattr dependency to extras_require, only install if specified

### Removed

* Unused python-magic, beatifulsoup4 python dependencies

## [304.1] - 2021.01.05 

### Fixed

* Building assets for Ubuntu 20.04

## [303.1] - 2020.11.13 

### Changed

* Increase wget retries to 20 for symlink downloads

## [302.1] - 2020.10.13

### Changed

* gnureadline macos dependency to 8.0.0 for Python versions < 3.9

## [301.1] - 2020.09.16

### Added

* Remote builders for 16.04 v1, 20.04
* Asset builder for 20.04
* '--verbose' flag for `dx-mount-all-inputs`

### Changed

* Provide project-id in batch tsv file

## [300.1] - 2020.08.31

### Added

* Archival api wrappers

### Changed

* Hide `notebook` and `loupe-viewier` from `dx` help output

## [299.0] - 2020.08.26

### Fixed

* Macos tarball build

## [298.1] - 2020.07.29 

### Added

* Ubuntu 20.04 build targets

### Changed

* jackson-databind from 2.9.10.3 to 2.9.10.5

### Fixed

* API wrapper generation with Python 3
* `dx-clone-asset` when no project exists
* DXJava DXDataObject.Rename()

## [297.1] - 2020.07.22 

### Changed

* Python cryptography version >= 2.3

### Fixed

* `dx-clone-asset` with Python 3

### Removed

* Ubuntu 14.04 builds

## [296.0] - 2020.07.01 

### Added

* Examples for `dx find` with negative timestamp
* `dx build --from applet-xxx` for app
* --brief option to dx build for apps and applets

### Fixed

* Error handling during syntax check for dx build

### Changed

* Python 2.7 example applets to use Python 3
* Commit dxpy version during release

## [295.1] - 2020.05.19

### Fixed

* `dx get` for applets with `python3` interpreter
* `dx-upload-all-outputs ---xattr-properties` parsing

### Changed

* Python 2.7 example applets to use Python 3
* Commit dxpy version during release

## [294.0] - 2020.04.30 

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

## [290.1] - 2019.11.21

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

## [284.0] - 2019.06.13

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
