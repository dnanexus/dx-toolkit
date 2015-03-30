DNAnexus Platform SDK
=====================

**To build `dx-toolkit` from source, see https://wiki.dnanexus.com/Downloads.**

* **Found a bug? See [Reporting Bugs](#reporting-bugs) below.**

`dx-toolkit` contains the DNAnexus API language bindings and utilities
for interacting with the DNAnexus platform.

See https://wiki.dnanexus.com/ and http://autodoc.dnanexus.com/ for relevant
documentation.

Using the toolkit on your system
--------------------------------

First, see the section "Runtime dependencies" below and install the appropriate
dependencies for your platform.

After unpacking the toolkit, initialize your environment by sourcing this file:

```
source dx-toolkit/environment
```

You will then be able to use `dx` (the [DNAnexus Command Line
Client](https://wiki.dnanexus.com/Command-Line-Client/Quickstart)) and other
utilities, and you will be able to use DNAnexus API bindings in the supported
languages.

Supported languages
-------------------

* Python (requires Python 2.7)
* C++

If you wish to use bindings for the other supported languages (Java, Perl, R,
Ruby), please see the [DNAnexus developer
portal](https://wiki.dnanexus.com/Developer-Portal) for specific instructions
for your language. In general you can either build from the source
distribution, or install a prebuilt package specific to the language.

Runtime dependencies
--------------------

### Ubuntu 14.04

    sudo apt-get install libboost-filesystem1.55.0 libboost-program-options1.55.0 \
        libboost-regex1.55.0 libboost-system1.55.0 libboost-thread1.55.0 libcurl3 \
        libbz2-1.0 zlib1g python-fuse python-pyxattr

### Ubuntu 12.04

    sudo apt-get install libboost-filesystem1.48.0 libboost-program-options1.48.0 \
        libboost-regex1.48.0 libboost-system1.48.0 libboost-thread1.48.0 libcurl3 \
        libbz2 zlib1g python-fuse python-pyxattr

### Ubuntu 10.04

Install Python2.7. Python 2.7 is not available natively on Ubuntu 10.04, but
Felix Krull maintains the [deadsnakes
PPA](https://launchpad.net/~fkrull/+archive/deadsnakes), which includes a build
for Ubuntu 10.04. You can install Python from there as follows (as root):

    echo "deb http://ppa.launchpad.net/fkrull/deadsnakes/ubuntu lucid main" > /etc/apt/sources.list.d/deadsnakes.list
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 5BB92C09DB82666C
    apt-get install --yes python2.7 python-pip python-setuptools-deadsnakes

Install boost 1.48 or higher (at least the `thread` and `regex`
libraries). This version of boost is not available natively on
Ubuntu 10.04. You can use the script `build/lucid_install_boost.sh`,
which installs it into `/usr/local/lib`.

Then:

    sudo apt-get install libcurl3 libbz2 zlib1g

### CentOS/RHEL 5.x/6.x

Install Python 2.7. Python 2.7 is not available natively on CentOS/RHEL
5 or 6. You can use the script `build/centos_install_python27.sh`, which
installs it into `/usr/local/bin`. (Run the script as root.)

Notes:

  - On CentOS/RHEL 5.x, two of the utilities, `dx-contigset-to-fasta`
    and `dx-reads-validator`, will not function correctly, as some of
    the library versions are too old.

  - Tested on CentOS 5.4 and CentOS 6.2.

### OS X

Install the [Command Line Tools for XCode](https://developer.apple.com/downloads/). (Free registration required with Apple)

Install the following packages, either from source or via [Homebrew](http://mxcl.github.com/homebrew/), [Fink](http://www.finkproject.org/), or [MacPorts](http://www.macports.org/):

* Boost >= 1.50 (`sudo port install boost` or `brew install boost`)
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

Reporting Bugs
--------------

Please use [GitHub](https://github.com/dnanexus/dx-toolkit/issues) to
report bugs, post suggestions, or send us pull requests.
