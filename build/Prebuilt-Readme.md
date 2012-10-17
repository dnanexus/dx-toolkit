DNAnexus Platform Toolkit
=========================

**To build ```dx-toolkit``` from source, see http://wiki.dnanexus.com/DNAnexus-SDK.**

```dx-toolkit``` contains the DNAnexus API language bindings and utilities for
interacting with the DNAnexus platform.

See http://wiki.dnanexus.com/ and http://autodoc.dnanexus.com/ for relevant
documentation.

Using the toolkit on your system
--------------------------------

First, see the section "Runtime dependencies" below and install the appropriate
dependencies for your platform.

After unpacking the toolkit, initialize your environment by sourcing this file:

```
source dx-toolkit/environment
```

You will then be able to use ```dx``` (the [DNAnexus Command Line
Client](http://wiki.dnanexus.com/Command-Line-Client/Quickstart)) and other
utilities, and you will be able to use DNAnexus API bindings in the supported
languages.

Supported languages
-------------------

* Python (requires Python 2.7)
* C++
* Perl
* Java
* Ruby (FIXME)

Runtime dependencies
--------------------

### Ubuntu 12.04

    sudo apt-get install libboost-regex1.46.1 libboost-thread1.46.1

### Ubuntu 10.04

    sudo apt-get install libboost-regex1.40.0 libboost-thread1.40.0 libgomp1

### OS X

Install the [Command Line Tools for XCode](http://wiki.dnanexus.com/DNAnexus-SDK). (Free registration required with Apple)

Install the following packages, either from source or via [Homebrew](http://mxcl.github.com/homebrew/), [Fink](http://www.finkproject.org/), or [MacPorts](http://www.macports.org/):

* Boost >= 1.50 (```sudo port install boost``` or ```brew install boost```)
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
