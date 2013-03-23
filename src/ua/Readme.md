# DNAnexus Upload Agent

This directory contains the source code for the DNAnexus Upload Agent (UA).
The UA is a command-line program for performing fast, robust file uploads
to the platform. The UA is written in C++ and runs on Linux, Mac OS X, and
Windows.

## Building

To build the UA, simply run `make` in this directory. This will produce a
subdirectory called `build/`, containing the `ua` executable.

On Linux, the resulting executable is statically linked. On Mac OS X, the
executable is statically linked with the exception of two libraries: libgcc
and libstdc++. As a result, the `libstdc++.6.dylib` and `libgcc_s.1.dylib`
files in the `build/` directory must be distributed along with the Mac
executable.

## Dependencies

The UA depends on the following libraries:

* [libcurl](http://curl.haxx.se/libcurl/) for HTTP requests. In particular,
  a custom statically-linked libcurl must be built, and this relies on:
  * c-ares, an asynchronous DNS library (Ubuntu package libc-ares-dev; Mac
    port c-ares);
  * OpenSSL for HTTPS;
* various [Boost](http://www.boost.org/) libraries, including Thread,
  Lexical Cast, Program Options, Filesystem, System, and Regex;
* [zlib](http://zlib.net/) for compression;
* libmagic, a library to recognize the MIME type of the input file(s) (Mac port "file")
* the dxjson and dxcpp libraries for communicating with the platform API.
