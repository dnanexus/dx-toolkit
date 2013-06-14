Java Parallelism Example App
============================

This app is a simple example that demonstrates how to spawn parallel subjobs
from Java code and coordinate them using job-based object references (JBORs).
The app concatenates the input file to itself `numSubtasks` times. (default: 5)

Provided you have set up your environment for building Java apps (see [the Java
tutorial on the
wiki](https://wiki.dnanexus.com/Developer-Tutorials/Java/Java)), you can build
this app as follows (supply a relative path to this directory,
`parallelism_java`, if needed):

```
dx-build-applet .
```

Then you can run the app with:

```
echo "Here is an input file line." > my_test_input
dx upload my_test_input
dx run --watch ./parallelism_test -iinput_file=my_test_input
```

Structure of this app
---------------------

The app is divided into three entry points, which you can find in `code.sh`:

* `main` invokes a Java program `DXParallelismExample` that spawns all the
  other subtasks. See `src/DXParallelismExample.java` for the implementation.

* `process` is run `numSubtasks` times in parallel and is implemented as a bash
  function. In this example app it just copies the file data directly from the
  input to the output. The `process` entry point takes two inputs (an integer
  `index` and a file ID `input_file`) and produces one output (a file ID
  `output_file`).

* `postprocess` waits until all the `process` tasks have finished. In this
  example app it just concatenates all their outputs. The `postprocess` entry
  point is also implemented in bash. It takes one input (an array of files,
  `process_outputs`) and produces one output (a single file,
  `combined_output`).
