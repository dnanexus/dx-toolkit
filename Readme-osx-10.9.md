Building on OS X 10.9
=====================

**NOTE:** This document is intended for developers who wish to build the dx-toolkit SDK and command-line tools from source.

Instead of building from source, most users can install the prebuilt DNAnexus Platform SDK for OS X (a.k.a. dx-toolkit) release available for download at:

https://wiki.dnanexus.com/Downloads

### Setup steps
---------------

1. Install Xcode and the [Command Line Tools for XCode](https://developer.apple.com/downloads/). (Free registration required with Apple)

1. Install [MacPorts](http://www.macports.org/) for your version of OS X:

  https://www.macports.org/install.php

1. If you want your dx-toolkit build to be backwards-compatible on OS X 10.7, add these lines to ```/opt/local/etc/macports/macports.conf``` to ensure that your MacPorts Python build is compiled with 10.7 support:

    ```
    macosx_deployment_target            10.7
    MACOSX_DEPLOYMENT_TARGET            10.7
    ```

1. Run the MacPorts install and select commands to configure your build environment:

    ```
    sudo port install -s python27
    sudo port install cmake bison autoconf automake
    sudo port install boost -no_static
    sudo port select --set python python27
    sudo port install py27-pip py27-virtualenv
    sudo port select --set pip pip27
    sudo port select --set virtualenv virtualenv27
    ```

1. Clone the dx-toolkit repo, and build the SDK:
    ```
    cd dx-toolkit
    export CPATH=/opt/local/include
    make
    ```

### Upload agent build setup steps
----------------------------------

1. Install the upload agent build dependencies:

    ```
    sudo port install libmagic c-ares
    ```

1. Build upload agent:

    ```
    CC=clang CXX=clang++ VERSIONER_PERL_VERSION=5.16 make ua
    ```
