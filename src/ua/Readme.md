# DNAnexus Upload Agent

This directory contains the source code for the DNAnexus Upload Agent (UA).
The UA is a command-line program for performing fast, robust file uploads
to the platform. The UA is written in C++ and runs on Linux, Mac OS X, and
Windows.

## Build dependencies

Before building the Upload Agent, follow the instructions in the SDK Readme
for installing system build dependencies.

## Building

To build the UA, simply run `make` in this directory. This will build the
`ua` executable in this directory. The following methods can be used to
deploy the Upload Agent:

* `make install` will deposit `ua` in the `$DNANEXUS_HOME/bin` directory
* `make dist` will build a complete UA distribution for the current
  platform and place it in the `dist` subdirectory. On Linux, the
  distribution is a .tar.gz file; on Windows and OS X, there are two
  products: a .zip file and an installer.

The `ua` executable is statically linked on Linux. On Mac OS X and Windows,
the executable is dynamically linked, and the necessary libraries are
copied along with it.

## Dependencies

The UA depends on the following libraries. They are automatically
downloaded and installed when running `make`:

* [libcurl](http://curl.haxx.se/libcurl/) for HTTP requests. In particular,
  a custom statically-linked libcurl must be built, and this relies on:
  * c-ares, an asynchronous DNS library (Ubuntu package libc-ares-dev; Mac
    port c-ares);
  * OpenSSL or the native Windows SSL library for HTTPS;
* various [Boost](http://www.boost.org/) libraries, including Thread,
  Lexical Cast, Program Options, Filesystem, System, and Regex;
* [zlib](http://zlib.net/) for compression;
* libmagic, a library to recognize the MIME type of the input file(s) (Mac port "file")
* the dxjson and dxcpp libraries for communicating with the platform API.
