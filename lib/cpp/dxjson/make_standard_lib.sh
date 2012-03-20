#!/bin/bash
make clean
cmake .
make
(ls /usr/local/lib >/dev/null 2>/dev/null) || sudo mkdir /usr/local/lib
sudo cp libdxjson.a /usr/local/lib/
(ls /usr/local/include/dxjson >/dev/null 2>/dev/null) || sudo mkdir /usr/local/include/dxjson
sudo cp dxjson.h /usr/local/include/dxjson
sudo cp -R utf8 /usr/local/include/dxjson
