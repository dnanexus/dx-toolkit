#!/usr/bin/env python

'''
Runs Python integration tests and merges the resulting test coverage files.

To run a single test method:

   # Runs the method "test_basic" in class TestDXBashHelpers, in file
   # src/python/test/test_dx_bash_helpers.py
   build/run_python_integration_tests.py --tests \
           test.test_dx_bash_helpers.TestDXBashHelpers.test_basic

To run an entire test class:

    build/run_python_integration_tests.py --tests \
            test.test_dx_bash_helpers.TestDXBashHelper

To run all tests in a file:

    build/run_python_integration_tests.py --tests \
            test.test_dx_bash_helpers

If no arguments are given, all tests in src/python/test/ are run.
'''

from __future__ import print_function, unicode_literals

import argparse
import os
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    '--tests',
    help='Specify a specific test to run.',
    metavar='test.test_module.TestCase.test_method',
    nargs='*'
)

TOOLKIT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
PYTHON_DIR = os.path.join(TOOLKIT_ROOT_DIR, 'src', 'python')

os.environ['DNANEXUS_INSTALL_PYTHON_TEST_DEPS'] = 'yes'

def run():
    subprocess.check_call(["make", "python"], cwd=TOOLKIT_ROOT_DIR)

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
            cmd += ['discover', '--start-directory', 'test', '--verbose']

        # Setting COVERAGE_PROCESS_START is required to collect coverage for
        # subprocess calls to dx.py and friends:
        subproc_env = dict(
                os.environ,
                COVERAGE_PROCESS_START = os.path.join(PYTHON_DIR, '.coveragerc'),
                COVERAGE_FILE = os.path.join(PYTHON_DIR, '.coverage'))
        subprocess.check_call(cmd, cwd=PYTHON_DIR, env=subproc_env)

        subprocess.check_call(["coverage", "combine"], cwd=PYTHON_DIR)
    finally:
        os.unlink(site_customize_filename)

if __name__ == '__main__':
    args = parser.parse_args()
    run()
