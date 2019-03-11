from __future__ import print_function
import os
import sys
import tempfile
import subprocess
import contextlib
import shutil


@contextlib.contextmanager
def set_env(**environ):
    """
    Context manager generator to temporarily set the subprocess environment variables.

    Args:
        environ (dict): Environment variable to set

    Yields:
        An environment with environment variables set as specified.
        On exit, the environment will return to previous configuration.

    Examples:
        Usage 1: Set environment variable
        # inside environment
        >>> with set_env(PLUGINS_DIR=u'test/plugins'):
        ...    "PLUGINS_DIR" in os.environ
        True

        # outside environment
        >>> "PLUGINS_DIR" in os.environ
        False

        Usage 2: Unset environment variable
        >>> with set_env(PYTHONPATH=''):
        ...    print(os.environ["PYTHONPATH"])
        <BLANKLINE>

        Usage 3: Manipulate multiple variables
        >>> myenv = {"PLUGINS_DIR": u'test/plugins', "PYTHONPATH": u'some/python/path'}
        >>> with set_env(**myenv):
        ...   print(os.environ["PLUGINS_DIR"])
        ...   print(os.environ["PYTHONPATH"])
        test/plugins
        some/python/path
    """
    old_environ = dict(os.environ)
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)

@contextlib.contextmanager
def cd(new_path=None, temp_dir=None):
    """Context manager that changes the current working directory to `new_path`,
    yields, and then changes the working directory back to what it was prior to
    calling this function.
    Args:
        new_path (string): Optional, specify path to cd to
        temp_dir (string): Optional, specify temporary directory to create and
            cd to
    Note:
        If no args specified, cd() will create an arbitary temp dir and cd to it
        If arg is specified without a keyword, it will be assumed as 'new_path'.
    Yields:
        Upon entry, context will be set to the specified directory.
        Upon exit, directory specified in temp_dir or directory created when no
        args are specified is deleted. If new_path is specified, it is not deleted.
    Source: http://stackoverflow.com/questions/431684/how-do-i-cd-in-python
    Examples:
       with cd():
           do_the_thing
           # this will create a temp directory with a randomly
           # generated name, doe the thing, then delete the temp dir
       with cd(my_file_dir):
           do_the_thing
           # this will do the thing in my_file_dir and not delete the directory
       with cd(temp_dir=my_temp_dir):
           do_the_thing
           # this will create a temp dir with path my_temp_dir, do the thing,
           # then delete the temp dir
    """
    remove_folder = False
    if new_path is None:
        new_path = tempfile.mkdtemp(dir=temp_dir)
        remove_folder = True

    saved_path = os.getcwd()
    os.chdir(new_path)

    try:
        yield
    finally:
        os.chdir(saved_path)
        if remove_folder:
            shutil.rmtree(new_path)


@contextlib.contextmanager
def temp(*args, **kwargs):
    """Context manager that yields a temp file name and deletes the file
    before exiting.
    Args:
        *args: positional arguments passed to mkstemp
        **kwargs: keyword arguments passed to mkstemp
    Examples:
        >>> with temp() as fn:
        >>>     with open(fn, "wt") as out:
        >>>         out.write("foo")
    """
    _, fname = tempfile.mkstemp(*args, **kwargs)
    try:
        yield fname
    finally:
        if os.path.exists(fname):
            os.remove(fname)


@contextlib.contextmanager
def fifo(name=None):
    """
    Create a FIFO, yield it, and delete it before exiting.
    Args:
        name: The name of the FIFO, or None to use a temp name.
    Yields:
        The name of the FIFO
    """
    if name:
        os.mkfifo(name)
        yield name
    else:
        with temp() as name:
            os.mkfifo(name)
            yield name

    if os.path.exists(name):
        os.remove(name)


if __name__ == "__main__":
    import doctest

    test_failures = doctest.testmod()[0]
    if test_failures > 0:
        print("Encountered {0} failures".format(test_failures))
        sys.exit(1)
    else:
        print("All tests passed.")
