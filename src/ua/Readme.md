# DNAnexus Upload Agent

This directory contains the source code for the DNAnexus Upload Agent (UA).
The UA is a command-line program for performing fast, robust file uploads
to the platform. The UA is written in C++ and runs on Linux, Mac OS X, and
Windows.

## Building

To build the UA, simply run `make` in this directory. This will produce a
subdirectory called `build/`, containing the `ua` executable.

## Dependencies

The UA depends on the following libraries:

* [libcurl](http://curl.haxx.se/libcurl/) for HTTP requests

* various [Boost](http://www.boost.org/) libraries, including Thread,
  Lexical Cast, Program Options, and Filesystem.

* [zlib](http://zlib.net/) for compression

* the dxjson and dxcpp libraries for communicating with the platform API
