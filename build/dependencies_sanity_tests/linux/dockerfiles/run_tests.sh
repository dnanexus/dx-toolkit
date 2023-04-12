#!/bin/bash

set -e

if [[ "$DXPY_TEST_USING_PYENV" == "true" ]]; then
    eval "$(pyenv init -)"
fi

export DXPY_TEST_PYTHON_BIN=$(which python${DXPY_TEST_PYTHON_VERSION})

if [[ -z "$DXPY_TEST_PYTHON_BIN" ]]; then
    echo "Cannot determine Python executable path"
    exit 1
fi

python${DXPY_TEST_PYTHON_VERSION} -m pip install /dx-toolkit/src/python

if [[ "$DXPY_TEST_USING_PYENV" == "true" ]]; then
    pyenv rehash
fi

source /pytest-env/bin/activate
pytest --verbose /tests/dependencies_sanity_tests.py $@
