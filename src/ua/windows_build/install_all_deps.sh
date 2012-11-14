#!/bin/sh -ex
# This file downloads all the dependencies required for building UA in ~/sw/local
mkdir -p ${HOME}/sw/tmp
./build_curl_windows.sh ${HOME}/sw/tmp
./build_boost.sh
./install_file_zlib.sh
echo "Completed building all dependencies. Type 'make dist' to create a distribution for Windows"