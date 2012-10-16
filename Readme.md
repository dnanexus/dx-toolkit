DNAnexus Platform Toolkit
=========================

**To download pre-built packages for your platform, see http://wiki.dnanexus.com/DNAnexus-SDK.**

Welcome to the `dx-toolkit` repository! This repository contains the DNAnexus
API language bindings and utilities for interacting with the DNAnexus platform.

See http://wiki.dnanexus.com/ and http://autodoc.dnanexus.com/ for relevant
documentation.

Installing the toolkit
----------------------

First, see the section "Build dependencies" below and install the appropriate
dependencies for your platform.

To build the toolkit, simply run ```make```.

Then, initialize your environment by sourcing this file:

```
source dx-toolkit/environment
```

You will then be able to use ```dx``` (the [DNAnexus Command Line
Client](http://wiki.dnanexus.com/Command-Line-Client/Quickstart)) and other
utilities, and you will be able to use DNAnexus API bindings in the supported
languages.

Supported languages
-------------------

The Platform Toolkit contains API language bindings for the following
platforms:

* Python (requires Python 2.7)
* C++
* Perl
* Java
* Ruby (FIXME)

Build dependencies
------------------

The following packages are required to build the toolkit. You can avoid having
to install them by either downloading a compiled release from
http://wiki.dnanexus.com/DNAnexus-SDK, or by building only a portion of the
toolkit that doesn't require them.

### Ubuntu 12.04

    sudo apt-get install python-setuptools g++ cmake libboost-dev \
      libcurl4-openssl-dev libboost-regex-dev libboost-thread-dev \
      libboost-system-dev

TODO: Verify that this list is sufficient

### OS X

* Install the [Command Line Tools for XCode](http://wiki.dnanexus.com/DNAnexus-SDK) (Free registration required with Apple)
* The following packages can be installed either from their respective websites or via [Homebrew](http://mxcl.github.com/homebrew/), [Fink](http://www.finkproject.org/), or [MacPorts](http://www.macports.org/).
    * [CMake](http://www.cmake.org/cmake/resources/software.html) (```sudo port install cmake``` or ```brew install cmake```)
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

* **Note:** There is an incompatibility when using GCC 4.7.1 and Boost 1.49.
  Please use either the GCC 4.6 series or Boost 1.50+ in this case.

### TODO: Ubuntu 10.04, Fedora/RHEL/CentOS

Java bindings
-------------

The Java bindings are not built by default.

### Build dependencies

* Maven

### Building

    cd src/java
    mvn package

### Installing

    mvn install

Test dependencies
-----------------

TODO
