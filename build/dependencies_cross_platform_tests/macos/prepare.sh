#!/bin/bash

set -e

# Install Homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

if [ -f /opt/homebrew/bin/brew ]; then
    BREW_BIN="/opt/homebrew/bin/brew"
else
    BREW_BIN="/usr/local/bin/brew"
fi

if [ "$SHELL" == "/bin/bash" ]; then
    echo "eval \"\$($BREW_BIN shellenv)\"" >> ~/.bashrc
else
    echo "eval \"\$($BREW_BIN shellenv)\"" >> ~/.zprofile
fi

# Activate Homebrew
eval "$($BREW_BIN shellenv)"

# Install pyenv
brew install pyenv

# Cryptography requirements for Python 3.6
brew install openssl@1.1 rust

# Check if Python 3 is available
if [ -f /usr/bin/python3 ]; then
    PYTHON3_BIN="/usr/bin/python3"
else
    brew install python@3.11
    if [ -f /opt/homebrew/bin ]; then
        PYTHON3_BIN="/opt/homebrew/bin/python3.11"
    else
        PYTHON3_BIN="/usr/local/opt/python@3.11/bin/python3.11"
    fi
fi

# Install tests dependencies
$PYTHON3_BIN -m pip install pytest pexpect

# Download official installation packages
mkdir python_official
pushd python_official
curl -f -O https://www.python.org/ftp/python/3.12.1/python-3.12.1-macos11.pkg
curl -f -O https://www.python.org/ftp/python/3.11.3/python-3.11.3-macos11.pkg
curl -f -O https://www.python.org/ftp/python/3.10.11/python-3.10.11-macos11.pkg
curl -f -O https://www.python.org/ftp/python/3.9.13/python-3.9.13-macos11.pkg
curl -f -O https://www.python.org/ftp/python/3.8.10/python-3.8.10-macos11.pkg
curl -f -O https://www.python.org/ftp/python/3.7.9/python-3.7.9-macosx10.9.pkg
curl -f -O https://www.python.org/ftp/python/3.6.8/python-3.6.8-macosx10.9.pkg
popd
