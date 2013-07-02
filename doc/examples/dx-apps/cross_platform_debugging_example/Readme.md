# Cross-Platform Debugging Example

This app demonstrates use of separate Linux and Mac binaries to support running
the same app code locally in the execution harness (on Mac or Linux) or
remotely in the DNAnexus Platform Execution Environment. This document
describes some of the practices that are used to make the app code portable
between local runs and runs in the Platform.

You can run the demonstration app locally as follows (on Linux or Mac):

    $ dx-run-app-locally path/to/cross_platform_debugging_example -iname=You
    [...]
    Final output: greeting = "Hello, You!"

You can also build and run it in the Platform as follows:

    # Also supply --remote if on Mac
    $ dx build path/to/cross_platform_debugging_example
    $ dx run --watch ./cross_platform_debugging_example -iname=You

## Executables and libraries

This app contains the source for a C++ executable (`src/greeter/main.cpp`) and
a library on which it relies (`src/greeter/greeter.*`). Typing `make` in the
root of the app directory builds the executable. It also builds the library as
a dynamically linked library (`*.so` on Linux or `*.dylib` on Mac).

Linux artifacts are placed into `resources/usr/bin` and `resources/usr/lib`.
When running in the DNAnexus Execution Environment, these appear in `/usr/bin`
and `/usr/lib`, respectively (which are in the default search paths). When
running the app using `dx-run-app-locally` on Linux, `APPDIR/resources/usr/bin`
and `APPDIR/resources/usr/lib` are added to your `PATH` and your
`LD_LIBRARY_PATH`, respectively.

Therefore, in both cases, app code can invoke the executable just by name
(e.g., `greeter`, or `subprocess.check_call(["greeter", ...])` from a Python
app), and you do not need to use the full path `/usr/bin/greeter` or modify
either `PATH` or `LD_LIBRARY_PATH` by hand.

### App development on Mac

Typing `make` in the root of the app directory on a Mac builds an executable
and library and puts them in subdirectories of `mac_resources/`. When you run
the app using `dx-run-app-locally` on Mac, `APPDIR/mac_resources/usr/bin` is
added to your `PATH`, and `APPDIR/mac_resources/usr/lib` is added to your
`DYLD_FALLBACK_LIBRARY_PATH`, so that again, you can invoke the executable just
by name.

When you are satisfied with local testing runs, you can deploy to the Platform
using `dx build --remote cross_platform_debugging_example`, which
performs the build steps in the Execution Environment to obtain the appropriate
Linux artifacts. Built artifacts you had in `mac_resources/`, if any, are not
uploaded.

## Data files

This demonstration app also reads a data file that resides at
`resources/opt/mydata`. You can read such a file in a portable way by using the
path `${DX_FS_ROOT}/opt/mydata`. This resolves to `/opt/mydata` when running in
the Execution Environment, or `APPDIR/resources/opt/mydata` when running
locally.

In Python you can write `os.environ.get('DX_FS_ROOT', '') + '/opt/mydata'`.

(You can also use this technique to generate absolute paths to any resource on
the filesystem, including binaries or libraries where you don't wish to use the
conventions described above for whatever reason.)

## Input/output specification

### Inputs

* **Your name** ``name``: ``string``

### Outputs

* **A greeting to you** ``greeting``: ``string``
