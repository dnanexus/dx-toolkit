#!/usr/bin/env python

'''
Runs Python integration tests and merges the resulting test coverage files.

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

    # Somewhat hacky-- ensures that all subprocess calls to dx-* tools
    # load the coverage instrumentation so that their use of dxpy is
    # reflected in the final report.
    site_customize_filename = os.path.join(TOOLKIT_ROOT_DIR, 'share', 'dnanexus', 'lib', 'python2.7', 'site-packages', 'sitecustomize.py')
    with open(site_customize_filename, 'w') as site_customize_file:
        site_customize_file.write("import coverage; coverage.process_startup()\n")
    try:
        subprocess.check_call("rm -vf .coverage .coverage.*", cwd=PYTHON_DIR, shell=True)
        cmd = ['python', '-m', 'unittest']
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

        # Setting COVERAGE_PROCESS_START is required to collect coverage for
        # subprocess calls to dx.py and friends. Also, wrap values in str()
        # to avoid "environment can only contain strings" error on Windows:
        subproc_env[str('COVERAGE_PROCESS_START')] = str(os.path.join(PYTHON_DIR, '.coveragerc'))
        subproc_env[str('COVERAGE_FILE')] = str(os.path.join(PYTHON_DIR, '.coverage'))

        try:
            subprocess.check_call(cmd, cwd=PYTHON_TEST_DIR, env=subproc_env)
        except subprocess.CalledProcessError as e:
            print('*** unittest invocation failed with code %d' % (e.returncode,), file=sys.stderr)
            sys.exit(1)

        try:
            subprocess.check_call(["coverage", "combine"], cwd=PYTHON_DIR)
        except subprocess.CalledProcessError as e:
            print('*** coverage invocation failed with code %d' % (e.returncode,), file=sys.stderr)
            sys.exit(1)
        except OSError:
            print("*** coverage invocation failed: no coverage file found; please install coverage v3.7.1",
                  file=sys.stderr)
    finally:
        os.unlink(site_customize_filename)

if __name__ == '__main__':
    args = parser.parse_args()
    run()
