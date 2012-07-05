#!/bin/bash
root=`dirname "$0"`
cd $root
make clean
cmake .
make
sudo mkdir -p /usr/local/lib
sudo cp libdxjson.a /usr/local/lib/
sudo mkdir -p /usr/local/include/dxjson
sudo cp dxjson.h /usr/local/include/dxjson
sudo cp -R utf8 /usr/local/include/dxjson
