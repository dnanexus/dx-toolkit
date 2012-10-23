#!/bin/sh -ex
pwd
cd $1
pwd
SWPATH=${HOME}/sw/local
cp $SWPATH/curl/bin/libcurl-4.dll .
cp $SWPATH/file-5.03-bin/bin/magic1.dll .
cp $SWPATH/zlib-1.2.3-bin/bin/zlib1.dll .
cp $SWPATH/regex-2.7-bin/bin/regex2.dll .
cp /bin/msys-1.0.dll .