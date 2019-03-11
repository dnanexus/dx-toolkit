from __future__ import print_function
import os
import sys
import tempfile
import subprocess
import contextlib


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


class cd:
    """
    Context manager for changing the current working directory

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

    def __init__(self, new_path=None, temp_dir=None):
        if new_path is not None:
            self.newPath = new_path
            self.removeFolder = False
        else:
            self.newPath = tempfile.mkdtemp(dir=temp_dir)
            self.removeFolder = True

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)
        if self.removeFolder:
            subprocess.check_call(['rm', '-rf', self.newPath], shell=False)


if __name__ == "__main__":
    import doctest

    test_failures = doctest.testmod()[0]
    if test_failures > 0:
        print("Encountered {0} failures".format(test_failures))
        sys.exit(1)
    else:
        print("All tests passed.")
