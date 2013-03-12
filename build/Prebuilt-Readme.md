DNAnexus Platform SDK
=====================

**To build ```dx-toolkit``` from source, see http://wiki.dnanexus.com/Downloads.**

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

Runtime dependencies
--------------------

### Ubuntu 12.04

    sudo apt-get install libboost-filesystem1.48.0 libboost-program-options1.48.0 \
        libboost-regex1.48.0 libboost-system1.48.0 libboost-thread1.48.0 libcurl3 \
        libbz2 zlib1g

### Ubuntu 10.04

Install Python2.7. Python 2.7 is not available natively on Ubuntu 10.04, but
Felix Krull maintains the [deadsnakes
PPA](https://launchpad.net/~fkrull/+archive/deadsnakes), which includes a build
for Ubuntu 10.04. You can install Python from there as follows (as root):

    echo "deb http://ppa.launchpad.net/fkrull/deadsnakes/ubuntu lucid main" > /etc/apt/sources.list.d/deadsnakes.list
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 5BB92C09DB82666C
    apt-get install --yes python2.7 python-pip python-setuptools-deadsnakes

Install boost 1.48 or higher (at least the ```thread``` and ```regex```
libraries). This version of boost is not available natively on Ubuntu 10.04.
You can use the script ```build/lucid_install_boost.sh```, which installs it
into ```/usr/local/lib```.

Then:

    sudo apt-get install libcurl3 libbz2 zlib1g

### CentOS 5.x/6.x

Install Python 2.7. Python 2.7 is not available natively on CentOS 5 or 6. You
can use the script ```build/centos_install_python27.sh```, which installs it
into ```/usr/local/bin```.

Install boost 1.48 or higher (at least the ```thread``` and ```regex```
libraries). This version of boost is not available natively on CentOS 5 or 6.
You can use the script ```build/centos_install_boost.sh```, which installs it
into ```/usr/local/lib```.

Then:

    yum install libcurl

Notes:

- On CentOS 5.x, two of the utilities, ```dx-contigset-to-fasta``` and
  ```dx-reads-validator```, will not function correctly, as some of the library
  versions are too old.

- Tested on CentOS 5.4 and CentOS 6.2.

### OS X

Install the [Command Line Tools for XCode](https://developer.apple.com/downloads/). (Free registration required with Apple)

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

Ruby bindings
----------
The Ruby package is called `dxruby`. It must be built before it can be used.

### Build dependencies

* Ruby 1.8+
* rubygems

#### Ubuntu
Use `apt-get install rubygems` to build with Ruby 1.8, or `apt-get install ruby1.9.3 make` to build with Ruby 1.9.

#### OS X
On OS X, dependencies may fail to install using Apple Ruby and the XCode toolchain. Instead, use `brew install ruby` to
install the [Homebrew](http://mxcl.github.com/homebrew/) Ruby 1.9.

### Building

    make ruby

### Using the package

The `environment` file will prepend to your `GEM_PATH` (see [Using the toolkit on your system](#using-the-toolkit-on-your-system)). To use `dxruby`, run:

```
require 'rubygems'
require 'dxruby'
```

In Ruby 1.9, `require 'rubygems'` is not necessary.
