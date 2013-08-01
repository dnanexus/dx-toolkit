#!/usr/bin/env python

import argparse
import os
import subprocess

parser = argparse.ArgumentParser(description="Runs Python integration tests and merges the resulting test coverage files.")

TOOLKIT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
PYTHON_DIR = os.path.join(TOOLKIT_ROOT_DIR, 'src', 'python')

env = os.environ.copy()
env['COVERAGE_PROCESS_START'] = os.path.join(PYTHON_DIR, '.coveragerc')

def run():
    # Somewhat hacky-- ensures that all subprocess calls to dx-* tools
    # load the coverage instrumentation so that their use of dxpy is
    # reflected in the final report.
    site_customize_filename = os.path.join(TOOLKIT_ROOT_DIR, 'share', 'dnanexus', 'lib', 'python2.7', 'site-packages', 'sitecustomize.py')
    with open(site_customize_filename, 'w') as site_customize_file:
        site_customize_file.write("import coverage; coverage.process_startup()\n")
    try:
        subprocess.check_call("rm -vf .coverage build/*/.coverage*", cwd=PYTHON_DIR, shell=True)
        subprocess.check_call(["./setup.py", "nosetests", "--cover-inclusive", "--ignore-files=(pyopenssl|ntlmpool)"], cwd=PYTHON_DIR, env=env)
        subprocess.check_call("mv build/*/.coverage* .", cwd=PYTHON_DIR, shell=True)
        subprocess.check_call(["coverage", "combine"], cwd=PYTHON_DIR)
    finally:
        os.unlink(site_customize_filename)

if __name__ == '__main__':
    args = parser.parse_args()
    run()
