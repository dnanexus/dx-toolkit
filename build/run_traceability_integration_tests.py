#!/usr/bin/env python3

'''
Runs Python integration tests tagged for inclusion in the traceability matrix.
'''

from __future__ import print_function, unicode_literals

import os
import platform
import subprocess
import sys

TOOLKIT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
PYTHON_DIR = os.path.join(TOOLKIT_ROOT_DIR, 'src', 'python')
PYTHON_TEST_DIR = os.path.join(PYTHON_DIR, 'test')

os.environ['DNANEXUS_INSTALL_PYTHON_TEST_DEPS'] = 'yes'
os.environ['DX_USER_CONF_DIR'] = TOOLKIT_ROOT_DIR + "/dnanexus_config_relocated"

def run():

    cmd = ['py.test', '-vv', '-s', '-m', 'TRACEABILITY_MATRIX', 'src/python/test/']

    subproc_env = dict(os.environ)

    try:
        subprocess.check_call(cmd, cwd=TOOLKIT_ROOT_DIR, env=subproc_env)
    except subprocess.CalledProcessError as e:
        print('*** unittest invocation failed with code %d' % (e.returncode,), file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    run()
