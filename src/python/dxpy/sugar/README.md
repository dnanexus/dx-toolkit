# `dxpy.sugar`

The `sugar` package provides some utilities to simplify common tasks encountered when using `dxpy`, especially when writing app(let)s.

## Modules

### `dxpy.sugar`

This module provides a few useful functions that don't fit into any of the submodules.

`in_worker_context` returns `True` if the script is running on a cloud worker, and `requires_worker_context` is a decorator that will throw an exception unless the function that it wraps is running on a cloud worker.

The `get_log` function returns a `logging.Logger` instance that is configured to be used on a worker.

The `available_memory` function returns the amount of available memory on the local machine. It takes as an argument the scale to use (K, M, or G).

```python
from dxpy.sugar import *

@requires_worker_context
def only_run_on_worker():
    log = get_log("worker")
    log.info(f"This worker has {available_memory("G")} GB memory")

@dxpy.entry_point("main")
def main():
    if in_worker_context():
        only_run_on_worker()
```

### `dxpy.sugar.chunking`

This module provides functions for partitioning collections for parallel processing. Currently, it has a single function, `divide_dxfiles_into_chunks`, which divides a collection of `dxpy.DXFile`s using the [LPT](https://en.wikipedia.org/wiki/Longest-processing-time-first_scheduling) algorithm.

```python
import dxpy
from dxpy.sugar import chunking

@dxpy.entry_point("main")
def main(my_files):
    # use subjobs to process files in chunks of ~5MB
    for chunk in chunking.divide_dxfiles_into_chunks(my_files, 5):
        dxpy.new_dxjob({"files": chunk}, "process_files")


@dxpy.entry_point("process_files")
def process_files(files):
    ...
```

Note that `divide_dxfiles_into_chunks` uses the `system/describeDataObjects` API call to get the sizes of all the files. If you know the sizes ahead of time, you can provide that information to avoid an extra API call:

```python
import dxpy
from dxpy.sugar import chunking

@dxpy.entry_point("main")
def main(my_files, my_file_sizes):
    # Provide a tuple of (file, size) and also the `file_size_key` argument, which is a function
    # that extracts the size from each tuple.
    for chunk in chunking.divide_dxfiles_into_chunks(
        zip(my_files, my_file_sizes), 5, lambda job: job[1]
    ):
        dxpy.new_dxjob({"files": [job[0] for job in chunk]}, "process_files")
```        

### `dxpy.sugar.context`

This module provides some useful context managers.

`UserContext` enables switching to a different user context than the user who has run this job, which requires having an API token.

`set_env` enables setting some environment variables temporarily.

`cd` temporarily changes the working directory. If a path is not specified, then a random temporary directory is created and is deleted when exiting the context manager.

`fifo` creates a temporary FIFO that is deleted with the context manager exits. If a path is not specified, then a temporary file is created and is deleted when exiting the context manager.

`tmpdir` creates a temporary directory and `tmpfile` creates a temporary file. In both cases the path is deleted when exiting the context manager.

```python
from dxpy.sugar import context, processing

API_TOKEN = "XYZ123"
PATH = '/some/path'
BAM = "mysample.bam"

# Align FASTQ files that are only accessible to a different user who has provided 
# `API_TOKEN`. Calls the "bwa_align" script which is installed at `PATH`. FASTQ files
# are streamed to FIFOs in a temp directory, and BWA output is stored to `BAM`.
with context.UserContext(API_TOKEN), \
        set_env({"PATH": PATH}) \
        tmpdir() as tmp_dir:
    with cd(tmp_dir):
        with tmpfile("fq_fifo1", dir=tmp_dir) as fq_fifo1, \
             tmpfile("fq_fifo2", dir=tmp_dir) as fq_fifo2:
        with fifo(tmp_file) as tmp_fifo:
            # stream the contents of fastq files to FIFOs
            dl_procs = [
                processing.run("dx cat mysample.fq", block=False, stdout=fq_fifo1),
                processing.run("dx cat mysample.fq", block=False, stdout=fq_fifo2)
            ]
            # run BWA using the FIFOs as input
            processing.run(f"bwa_align myindex {fq_fifo1} {fq_fifo1}", stdout=bam)
            # wait for dx processes to finish
            for proc in dl_procs:
                proc.block()
```

`tmpdir` has a `change_dir` argument that defaults to `False`; when set to `True` then it is equivalent to:

```python
with tmpdir() as tmp:
    with cd(tmp):
        ...
```

### `dxpy.sugar.filetools`

This module provides utilities for working with file names and paths. Currently it has a single function, `get_file_parts` that splits a filename into its constituent parts: `(prefix, suffixes, mates)`, where `prefix` is the file basename with no suffixes, `suffixes` is a tuple of file suffixes without any leading separators, and `mates` is a tuple of mate designators if the files use a common FASTQ naming convention. If `get_file_parts` is called with multiple files, then `prefix` only includes characters in common between all the files.

```python
>>> from dxpy.sugar.filetools import get_file_parts
>>> get_file_parts("foo.1.fastq", "foo.2.fastq")
# => ("foo", ("fastq",), (".1", ".2"))
>>> get_file_parts("foo_1.fq.gz")
# => ("foo", ("fq", "gz"), ("_1"))
>>> get_file_parts("foo.txt")
```

### `dxpy.sugar.objects`

This module provides functions for working with `dxpy` objects.

All functions have an `exists` argument that may be `None`, `True`, or `False`. If it `True`, then an error is raised if the object *does not* exist, and if it is `False` then an error is raised if the object *does* exist.

`get_project` looks up a project by name with options for specifying the minimum access level (`level`) and region (`region`). If the project doesn't exist and `create` is `True` then the project is created. Similarly, `ensure_folder` creates a folder in a project if it does not already exist. It returns a (non-recursive) list of all the objects in the folder.

The remaining functions are convenience methods for looking up objects using a name, ID, dxlink, or `dxpy` object. The functions that return `dxpy.DXDataObject` objects all take an optional project and use the current project if it is not specified:

* `get_data_object` looks up any type of data object (e.g. file, record, applet, workflow).
* `get_app_or_applet` looks up an app or applet, while `get_app` only looks up apps.
* `get_workflow` looks up a workflow or globalworkflow, while `get_globalworkflow` only looks up globalworkflows.

### `dxpy.sugar.processing`

This module provides convenience methods for executing subprocesses. `run` is the primary function and returns a `dxpy.sugar.processing.core.Processes` object. `sub` is a convenience method that blocks until the subprocess is complete and then returns `stdout`.

#### Example 1: Pipe multiple commands together and print output to file
           
```python
from dxpy.sugar.processing import run
example_cmd1 = ['dx', 'download', 'file-xxxx']
example_cmd2 = ['gunzip']
out_f = "somefilename.fasta"
proc = run([example_cmd1, example_cmd2], stdout=out_f)
print(proc.error)
# executes `dx download file-xxxx | gunzip > somefilename.fasta` and
# prints the contents of the process' stderr
```

#### Example 2: Pipe multiple commands together and return output

```python
from dxpy.sugar.processing import run
example_cmd1 = ['gzip', 'file.txt']
example_cmd2 = ['dx', 'upload', '-', '--brief']
file_id = sub([example_cmd1, example_cmd2], block=True)
# executes `gzip file.txt | dx upload - --brief` and returns the output 
```

#### Example 3: Run a single command with output to file

```python
run('echo "hello world"', stdout='test2.txt')
```

#### Example 4: A command failing mid-pipe should return CalledProcessedError

```python
run([['echo', 'hi:bye'], ['grep', 'blah'], ['cut', '-d', ':', '-f', '1']])
```

results in the following stack trace:

```
Traceback (most recent call last):
    ...
    CalledProcessError: Command '['grep', 'blah']' returned non-zero
        exit status 1
```

### `dxpy.sugar.transfers`

This module provides convenience functions for uploading and downloading files, as well as the `Downloader` and `Uploader` classes for managing multiple uploads/downloads in parallel.

`Downloader` and `Uploader` have the same API; the primary difference is that `Downloader` takes `dxpy.DXFile` or dxlink inputs and has `pathlib.Path` outputs, while `Uploader` takes `Path` inputs and has dxlink outputs. The number of parallel downloads/uploads is controlled by the `max_parallel` argument to the constructor and defaults to the number of CPUs on the local machine, with a maximum of 8 to avoid overloading the API server.

* `enqueue_file`: enqueues a file to be downloaded/uploaded. The file is associated with a name. If called multiple times with the same name, then the output will be a list.
* `enqueue_list`: enqueues a `list` of files to be downloaded/uploaded.
* `enqueue_dict`: enqueues a `dict` in which the key is a name and the value is either a file or a list of files.

Additional keyword arguments may be passed to the `enqueue*` functions depending on the type of file being downloaded. For example, if a file with a `.gz` suffix is downloaded then it is unzipped unless `skip_decompress=True` is specified, and if a file ends with `.tar` (or `.tar.gz` or `.tgz`) then it is also unarchived unless `skip_unpack=True` is specified. Keyword arguments can also be passed to the `Uploader`/`Downloader` constructor to change the default for all files that are enqueued.

Once all files are enqueued, then calling `wait()` blocks until all files are downloaded and then returns a `dict` in which the key is the name used when calling the `enqueue*` functions, and the value is a file or lists of files.

```python
import dxpy
from dxpy.sugar.transfers import Downloader, Uploader

@dxpy.entry_point("main")
def main(my_file1, my_file2, my_tar_file, my_gz_file_array):
    with Downloader() as downloader:
        downloader.enqueue_file("my_files", my_file1)
        downloader.enqueue_file("my_files", my_file2)
        downloader.enqueue_file("my_tar", my_tar_file, skip_unpack=True)
        downloader.enqueue_list("my_unzipped_files", my_gz_file_array)
        inputs = downloader.wait()
    
    assert len(inputs["my_files"]) == 2
    # since skip_unpack=True, the Path should point to a file, not a directory
    assert inputs["my_tar"].is_dir() is False
    for path in inputs["my_unzipped_files"]:
        # gz files are automatically unzipped and so should no longer have .gz suffix
        assert not path.suffix != ".gz"
    
    ...

    with Uploader(skip_compress=True) as uploader:
        # my_output is one of the app's output parameters
        uploader.enqueue_file("my_output", output_file)
        return uploader.wait()
```

## Contributing

If you have a function you think would be generally useful for developing app(let)s and other scripts based on `dxpy`, please submit a pull request.