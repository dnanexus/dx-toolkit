DNAnexus Platform Toolkit
=========================

Welcome to the `dx-toolkit` repository! This repository contains the DNAnexus API language bindings and utilities for interacting with the DNAnexus platform.

See http://wiki.dnanexus.com/ and http://autodoc.dnanexus.com/ for relevant documentation.

## Using the toolkit on your system
Initialize your environment by sourcing this file:

```
source dx-toolkit/environment
```

After initialization, you will be able to use ```dx``` (the [DNAnexus Command Line Client](http://wiki.dnanexus.com/Command-Line-Client/Quickstart)) and other utilities; and you will be able to use DNAnexus API bindings in supported languages.

Supported languages:

* Python
* C++
* Perl
* Java
* Ruby (FIXME)

## Building the toolkit

To build the toolkit, simply run ```make```.

### Build dependencies

The following packages are required to build the toolkit. You can avoid having to install them by either downloading a
compiled release from https://github.com/dnanexus/dx-toolkit/downloads, or by building only a portion of the toolkit
that doesn't require them.

#### Ubuntu 12.04

TODO: Necessary but not sufficient list

    python-setuptools g++ cmake libboost-dev libcurl4-openssl-dev libboost-regex-dev libboost-thread-dev libboost-system-dev

#### Ubuntu 10.04

#### Fedora/RHEL/CentOS

#### OS X
* Command Line Tools for XCode (https://developer.apple.com/downloads/ - free registration required with Apple)
* The following packages can be installed either from their respective websites or via [Homebrew](http://mxcl.github.com/homebrew/), [Fink](http://www.finkproject.org/), or [MacPorts](http://www.macports.org/).
    * CMake (http://www.cmake.org/cmake/resources/software.html) (```sudo port install cmake``` or ```brew install cmake```)
    * Boost >= 1.49 (```sudo port install boost``` or ```brew install boost```)
    * GCC >= 4.6
        * On MacPorts, install and select GCC with:

            ```
            sudo port install gcc47
            sudo port select --set gcc mp-gcc47
            ```

        * On Homebrew, install and select an up-to-date version of GCC with:

            ```
            brew install --enable-cxx https://raw.github.com/Homebrew/homebrew-dupes/master/gcc.rb
            export CC=gcc-4.7
            export CXX=g++-4.7
            ```

* **Note:** There is an incompatibility when using GCC 4.7.1 and Boost 1.49. Please use either the GCC 4.6 series or Boost 1.50+ in this case.

### Java bindings
The Java bindings are not built by default.

#### Build dependencies

* Maven

#### Building

    cd src/java
    mvn package

#### Installing

    mvn install

### Test dependencies
TODO

##Installing dependencies for prebuilt dx-toolkit
You can download a prebuilt archive of dx-toolkit [here](http://wiki.dnanexus.com/DNAnexus-SDK). Depending on your OS type, you may need to install some additional dependencies.

### Ubuntu 12.04
You can install dx-toolkit, and all related dependencies by installing our debian package. ```TODO: Instructions```

### Ubuntu 10.04
You will need to install following packages:
- libboost-regex
- libboost-thread
- libgomp1

Run this command to install them

```
sudo apt-get install libboost-regex1.40.0 libboost-thread1.40.0 libgomp1
```

### OS X
You will need Boost C++ libraries and GCC 4.6+ installed for running tools in precompiled dx-toolkit. Please see [OS X](#os-x) section above for instructions on how to install them.
