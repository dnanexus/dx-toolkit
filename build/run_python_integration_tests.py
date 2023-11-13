#!/usr/bin/env python3

'''
Runs Python integration tests

Assumes  `make python src_libs && source build/py_env/bin/activate`

If no arguments are given, all tests in src/python/test/ are run.

To run a single test method, e.g. test_basic in class TestDXBashHelpers,
in file src/python/test/test_dx_bash_helpers.py:

    $ build/run_python_integration_tests.py --tests test_dx_bash_helpers.TestDXBashHelpers.test_basic

To run an entire test class:

    $ build/run_python_integration_tests.py --tests test_dx_bash_helpers.TestDXBashHelpers

To run all tests in a file:

    $ build/run_python_integration_tests.py --tests test_dx_bash_helpers
'''

from __future__ import print_function, unicode_literals

import argparse
import os
import platform
import subprocess
import sys

parser = argparse.ArgumentParser(usage=__doc__)
parser.add_argument(
    '--tests',
    help='Specify a specific test to run.',
    metavar='test_module.TestCase.test_method',
    nargs='*'
)

TOOLKIT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
PYTHON_DIR = os.path.join(TOOLKIT_ROOT_DIR, 'src', 'python')
PYTHON_TEST_DIR = os.path.join(PYTHON_DIR, 'test')
os.environ['DNANEXUS_INSTALL_PYTHON_TEST_DEPS'] = 'yes'

def run():
    # Assumes `make python src_libs` has been run

    cmd = ['python3', '-m', 'unittest']
    if args.tests:
        cmd += ['-v'] + args.tests
    else:
        cmd += ['discover', '--start-directory', '.', '--verbose']

    if platform.system() == 'Windows':
        # Grab existing env vars with nt.environ, so that the SystemRoot var
        # isn't uppercased - because that can cause _urandom() errors. See:
        #   http://bugs.python.org/issue1384175#msg248951
        import nt
        subproc_env = dict(nt.environ)
    else:
        subproc_env = dict(os.environ)

    try:
        subprocess.check_call(cmd, cwd=PYTHON_TEST_DIR, env=subproc_env)
    except subprocess.CalledProcessError as e:
        print('*** unittest invocation failed with code %d' % (e.returncode,), file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    args = parser.parse_args()
    run()
