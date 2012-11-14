#!/bin/sh -ex

# This script builds boost libraries required for Upload Agent

cd ${HOME}/sw/local
pwd

wget "http://downloads.sourceforge.net/project/boost/boost/1.51.0/boost_1_51_0.tar.bz2"
bunzip2 boost_1_51_0.tar.bz2
tar xvf boost_1_51_0.tar
cd boost_1_51_0

./bootstrap.sh --with-toolset=mingw
sed -e s/gcc/mingw/ project-config.jam > project-config.jam
./b2 toolset=gcc --link=static --threding=multi --with-chrono --with-filesystem --with-thread --with-program_options --with-system --with-regex