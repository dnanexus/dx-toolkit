#!/bin/bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ -f /usr/bin/python3 ]; then
    PYTHON3_BIN="/usr/bin/python3"
else
    if [ -f /opt/homebrew/bin ]; then
        PYTHON3_BIN="/opt/homebrew/bin/python3.11"
    else
        PYTHON3_BIN="/usr/local/opt/python@3.11/bin/python3.11"
    fi
fi

$PYTHON3_BIN $SCRIPT_DIR/run_macos.py $@
