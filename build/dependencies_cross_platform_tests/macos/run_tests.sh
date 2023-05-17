#!/bin/bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

DX_TOOLKIT_DIR=$1
shift
TESTENV_DIR=$1
shift

if [[ "$DXPY_TEST_USING_PYENV" == "true" ]]; then
    eval "$(pyenv init -)"
fi

if [[ -z "$DXPY_TEST_BASE_PYTHON_BIN" ]]; then
    DXPY_TEST_BASE_PYTHON_BIN=$(which python${DXPY_TEST_PYTHON_VERSION})
fi

echo "Using $($DXPY_TEST_BASE_PYTHON_BIN --version 2>&1) (${DXPY_TEST_BASE_PYTHON_BIN})"


if [[ "$DXPY_TEST_PYTHON_VERSION" == "2" ]]; then
    $DXPY_TEST_BASE_PYTHON_BIN -m pip install --user virtualenv
    $DXPY_TEST_BASE_PYTHON_BIN -m virtualenv $TESTENV_DIR
else
    $DXPY_TEST_BASE_PYTHON_BIN -m venv $TESTENV_DIR
fi

source $TESTENV_DIR/bin/activate

export DXPY_TEST_PYTHON_BIN=$(which "python${DXPY_TEST_PYTHON_VERSION}")

if [[ -z "$DXPY_TEST_PYTHON_BIN" ]]; then
    echo "Cannot determine Python executable path"
    exit 1
fi

PYTHON_VERSION=$($DXPY_TEST_PYTHON_BIN --version 2>&1)

echo "Using venv with $PYTHON_VERSION ($DXPY_TEST_PYTHON_BIN)"

if echo "$PYTHON_VERSION" | grep -F ' 2.7.'; then
    if [[ ! -z "$DXPY_TEST_EXTRA_REQUIREMENTS" ]]; then
        LDFLAGS="-L$(brew --prefix openssl@1.1)/lib" CFLAGS="-I$(brew --prefix openssl@1.1)/include" $DXPY_TEST_PYTHON_BIN -m pip install -r $DXPY_TEST_EXTRA_REQUIREMENTS
    fi
    LDFLAGS="-L$(brew --prefix openssl@1.1)/lib" CFLAGS="-I$(brew --prefix openssl@1.1)/include" $DXPY_TEST_PYTHON_BIN -m pip install $DX_TOOLKIT_DIR
elif echo "$PYTHON_VERSION" | grep -F ' 3.6.'; then
    $DXPY_TEST_PYTHON_BIN -m pip install pip==20.3.4 setuptools==50.3.2 wheel==0.37.1
    if [[ ! -z "$DXPY_TEST_EXTRA_REQUIREMENTS" ]]; then
        LDFLAGS="-L$(brew --prefix openssl@1.1)/lib" CFLAGS="-I$(brew --prefix openssl@1.1)/include" $DXPY_TEST_PYTHON_BIN -m pip install -r $DXPY_TEST_EXTRA_REQUIREMENTS
    fi
    LDFLAGS="-L$(brew --prefix openssl@1.1)/lib" CFLAGS="-I$(brew --prefix openssl@1.1)/include" $DXPY_TEST_PYTHON_BIN -m pip install $DX_TOOLKIT_DIR
else
    if [[ ! -z "$DXPY_TEST_EXTRA_REQUIREMENTS" ]]; then
        $DXPY_TEST_PYTHON_BIN -m pip install -r $DXPY_TEST_EXTRA_REQUIREMENTS
    fi
    $DXPY_TEST_PYTHON_BIN -m pip install $DX_TOOLKIT_DIR
fi

hash -r

# We want to use system Python for running pytest
if [ -f /usr/bin/python3 ]; then
    MAIN_PYTHON_BIN="/usr/bin/python3"
else
    if [ -f /opt/homebrew/bin ]; then
        MAIN_PYTHON_BIN="/opt/homebrew/bin/python3.11"
    else
        MAIN_PYTHON_BIN="/usr/local/opt/python@3.11/bin/python3.11"
    fi
fi

$MAIN_PYTHON_BIN -m pytest --verbose ${SCRIPT_DIR}/../dependencies_cross_platform_tests.py $@