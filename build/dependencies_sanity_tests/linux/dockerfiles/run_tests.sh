#!/bin/bash

set -e

if [[ "$DXPY_TEST_USING_PYENV" == "true" ]]; then
    eval "$(pyenv init -)"
fi

export DXPY_TEST_PYTHON_BIN=$(which python${DXPY_TEST_PYTHON_VERSION})

echo "Using $($DXPY_TEST_PYTHON_BIN --version 2>&1) (${DXPY_TEST_PYTHON_BIN})"

if [[ -z "$DXPY_TEST_PYTHON_BIN" ]]; then
    echo "Cannot determine Python executable path"
    exit 1
fi

TMPDIR=$(mktemp -d -t dx-toolkit-XXXXXX)
cp -a /dx-toolkit $TMPDIR

$DXPY_TEST_PYTHON_BIN -m pip install $TMPDIR/dx-toolkit/src/python

if [[ "$DXPY_TEST_USING_PYENV" == "true" ]]; then
    pyenv rehash
fi

source /pytest-env/bin/activate
pytest --verbose /tests/dependencies_sanity_tests.py $@
