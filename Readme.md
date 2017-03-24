DNAnexus Platform SDK
=====================

* **To download pre-built packages for your platform, see https://wiki.dnanexus.com/Downloads.**

* **Found a bug? See [Reporting Bugs](#reporting-bugs) below.**

Welcome to the `dx-toolkit` repository! This repository contains the DNAnexus
API language bindings and utilities for interacting with the DNAnexus platform.

See https://wiki.dnanexus.com/ and http://autodoc.dnanexus.com/ for relevant
documentation.

Installing the toolkit from source
----------------------------------

First, see the section "Build dependencies" below and install the appropriate
dependencies for your platform.

To build the toolkit, simply run `make`.

Then, initialize your environment by sourcing this file:

```
source dx-toolkit/environment
```

You will then be able to use `dx` (the [DNAnexus Command Line
Client](https://wiki.dnanexus.com/Command-Line-Client/Quickstart)) and other
utilities, and you will be able to use DNAnexus API bindings in the supported
languages.

Supported languages
-------------------

The Platform SDK contains API language bindings for the following platforms:

* [Python](src/python/Readme.md) (requires Python 2.7)
* C++
* [Java](src/java/Readme.md) (requires Java 7 or higher)
* [R](src/R/Readme.md)

Javascript support lives in a separate repo,
[dnanexus/dx-javascript-toolkit.git](https://github.com/dnanexus/dx-javascript-toolkit).

Build dependencies
------------------

The following packages are required to build the toolkit. You can avoid having
to install them by either downloading a compiled release from
https://wiki.dnanexus.com/Downloads, or by building only a portion of the
toolkit that doesn't require them.

**Note:** There is a known incompatibility (in compiling dxcpp) when using GCC 4.7 with Boost 1.49. Please either use the GCC 4.6 series, or Boost 1.50+.

### Ubuntu 16.04

    sudo apt install make python-setuptools python-pip python-virtualenv python-dev \
      gcc g++ cmake libboost-all-dev libcurl4-openssl-dev zlib1g-dev libbz2-dev flex bison \
      openssl libssl-dev autoconf

### Ubuntu 14.04

    sudo apt-get install make python-setuptools python-pip python-virtualenv python-dev \
      g++ cmake libboost1.55-all-dev libcurl4-openssl-dev zlib1g-dev libbz2-dev flex bison \
      autoconf curl

### Ubuntu 12.04

    sudo apt-get install make python-setuptools python-pip python-dev \
      g++ cmake libboost1.48-all-dev libcurl4-openssl-dev zlib1g-dev libbz2-dev flex bison \
      autoconf curl
    sudo pip install --upgrade virtualenv

### Fedora

    yum install gcc gcc-c++ automake bison flex python python-pip \
      python-virtualenv boost-devel boost-static cmake openssl-devel \
      libcurl-devel bzip2-devel curl

This package set was tested on **Fedora 20**, which has the following package
versions (abbreviated list):

* gcc 4.8.2
* Python 2.7.5
* python-pip 1.4.1
* python-virtualenv 1.10.1
* boost 1.54.0
* cmake 2.8.12.1
* openssl 1.0.1e
* libcurl 7.32.0

### CentOS/RHEL 5.x/6.x

Install Python 2.7. Python 2.7 is not available natively on CentOS/RHEL
5 or 6. You can use the script `build/centos_install_python27.sh`, which
installs it into `/usr/local/bin`. (Run the script as root.)

Install boost 1.48 or higher (at least the `thread` and `regex`
libraries). This version of boost is not available natively on
CentOS/RHEL 5 or 6. You can use the script
`build/centos_install_boost.sh`, which installs it into
`/usr/local/lib`.

Then:

    yum install cmake libcurl-devel
    easy_install-2.7 pip
    pip-2.7 install virtualenv

Notes:

  - On CentOS/RHEL 5.x, one of the utilities, `dx-contigset-to-fasta`,
    will not function correctly, as some of the library versions are too
    old.

  - Tested on CentOS 5.4 and CentOS 6.2.

### OS X

Install the [Command Line Tools for XCode](https://developer.apple.com/downloads/). (Free registration required with Apple)

Install `pip` and `virtualenv` for Python:

    easy_install-2.7 pip
    pip-2.7 install virtualenv

Install the following packages from source or via [Homebrew](http://mxcl.github.com/homebrew/), [Fink](http://www.finkproject.org/), or [MacPorts](http://www.macports.org/):

* [CMake](http://www.cmake.org/cmake/resources/software.html) (`sudo port install cmake` or `brew install cmake`)
* Boost >= 1.49 (`sudo port install boost` or `brew install boost`)
* GCC >= 4.6
    * On MacPorts, install and select GCC with:

        ```
        sudo port install gcc47
        sudo port select --set gcc mp-gcc47
        ```

    * On Homebrew, install and select an up-to-date version of GCC with:

        ```
        brew tap homebrew/versions
        brew install gcc47
        export CC=gcc-4.7
        export CXX=g++-4.7
        ```
* bison >= 2.7, autoconf, automake
  * On Homebrew: `brew install bison autoconf automake`
  * On MacPorts: `sudo port install bison autoconf automake`

### Windows
Warning: Not all parts of the SDK are compatible with Windows. Install the following dependencies to build the Upload Agent:

* [MinGW](http://www.mingw.org/), including `mingw32-libz-dev`, `mingw-zip`, and [`mingw-regex`](http://sourceforge.net/projects/mingw/files/Other/UserContributed/regex/mingw-regex-2.5.1/).
* [NSIS](http://nsis.sourceforge.net/)

To generate the .dll dependencies required for Windows, run `make ua`, then `make pynsist_installer`, it is also possible to pass `DLL_DEPS_FOLDER=C:/folder/path/` as an argument to make

Upload Agent
------------
See the [Upload Agent Readme](https://github.com/dnanexus/dx-toolkit/blob/master/src/ua/Readme.md) for Upload Agent build documentation.

Reporting Bugs
--------------

Please use [GitHub](https://github.com/dnanexus/dx-toolkit/issues) to
report bugs, post suggestions, or send us pull requests.
