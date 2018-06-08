#!/bin/bash -ex

# Runs Python integration tests tagged for inclusion in the traceability matrix.

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOOLKIT_ROOT_DIR="${MY_DIR}/.."
export DNANEXUS_INSTALL_PYTHON_TEST_DEPS="yes"
export DX_USER_CONF_DIR="${TOOLKIT_ROOT_DIR}/dnanexus_config_relocated"

cd $TOOLKIT_ROOT_DIR
make python

source build/py_env/bin/activate
source environment

export PYTHONPATH="${TOOLKIT_ROOT_DIR}/src/python/test:${TOOLKIT_ROOT_DIR}/share/dnanexus/lib/python2.7/site-packages"
#py.test -m TRACEABILITY_MATRIX src/python/test/test_dx_bash_helpers.py::TestDXBashHelpers::test_basic -sv
py.test -vv -s -m TRACEABILITY_MATRIX ${TOOLKIT_ROOT_DIR}/src/python/test/
