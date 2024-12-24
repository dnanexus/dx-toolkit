DNAnexus Platform SDK
=====================

**To install the `dx` CLI and the Python SDK for your platform run `python3 -m pip install dxpy`**

* **Found a bug? See [Reporting Bugs](#reporting-bugs) below.**

Welcome to the `dx-toolkit` repository! This repository contains the DNAnexus
API language bindings and utilities for interacting with the DNAnexus platform.

See https://documentation.dnanexus.com/ and http://autodoc.dnanexus.com/ for relevant
documentation.

Installing the toolkit from source
----------------------------------

The recommended way to install the Python SDK and `dx` CLI of `dx-toolkit` locally is with `python3 -m pip install -e dx-toolkit/src/python`. 
Any changes made within this checkout will be reflected in the pip installed version. 

### Building inside docker 
To avoid lengthy installation of dependencies on your platform and simultaneous installations of development versions of `dx-toolkit` on the system, you can build `dx-toolkit` inside a docker container. 

1. Start `python:3.9-bullseye` in the interactive mode, mounting the repo you are working on (`<local_path_to_repo>/dx-toolkit`):

    ```
    # from root folder of dx-toolkit
    docker run -v `pwd`:/dx-toolkit -w /dx-toolkit -it --rm --entrypoint=/bin/bash python:3.9-bullseye
    ```
2. From the interactive shell install `dx-toolkit`.
    - **A.** Using local checkout:
        ```
        python3 -m pip install src/python/ --upgrade
        ```
    - **B.** Using remote branch, in this example specified in "@master":
        ```
        python3 -m pip install --upgrade 'git+https://github.com/dnanexus/dx-toolkit.git@master#egg=dxpy&subdirectory=src/python'
        ```
3. Log in, install dependencies(if needed) and use the container while developing. To rebuild, just save the work and run the step 2 again.

Supported languages
-------------------

The Platform SDK contains API language bindings for the following platforms:

* [Python](src/python/Readme.md) (requires Python 3.8 or higher)
* C++
* [Java](src/java/Readme.md) (requires Java 7 or higher)

Build dependencies for C++ and Java
------------------

**Note:** There is a known incompatibility (in compiling dxcpp) when using GCC 4.7 with Boost 1.49. Please either use the GCC 4.6 series, or Boost 1.50+.

### Ubuntu 22.04

    sudo apt install git openjdk-11-jre-headless maven python-is-python3 python3-venv python3-dev libssl-dev libffi-dev \
      flex bison build-essential cmake libboost-all-dev curl libcurl4-openssl-dev

### Ubuntu 20.04

    sudo apt install git make openjdk-11-jre-headless maven python-is-python3 python3-venv libssl-dev flex bison libffi-dev libboost-all-dev curl libcurl4-openssl-dev

Upload Agent
------------
See the [Upload Agent Readme](https://github.com/dnanexus/dx-toolkit/blob/master/src/ua/Readme.md) for Upload Agent build documentation.

Reporting Bugs
--------------

Please contact support@dnanexus.com for any bug reports or suggestions. 
