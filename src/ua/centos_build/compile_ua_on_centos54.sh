#!/bin/sh -ex

## - The following script is intended for compiling Upload Agent on older Kernel (e.g., >=2.6.9)
## - It has been *only* been tested on an EC2 CentOS 5.4 instance (AMI: ami-eb2273ae, region: us-west-1)
## - For more CentOS AMIs (old versions), see: http://support.rightscale.com/18-Release_Notes/MultiCloud_Image_and_RightImage_Release_Notes/2009-12-29

## - The script assumes you are logged as "root" into a freshly launched CentOS 5.4 EC2 instance (ami-eb2273ae)
##   and it does *NOT* clean up after itself (rather assumes that you will shutdown the instance anyway)
##
## - The script automates various hacks which I had to go thru as part of PTFM-6591. A super brief summary:
##   1) Compile git from source
##   2) Compile c-ares
##   3) Compile openssl
##   4) Compile libcurl
##   5) Compile libmagic
##   6) Copy g++44 to g++, and gcc44 to gcc (and export CC, CXX appropriately too)
##   7) Compile libboost (make sure that gcc v4.4 is being used)
##   8) Checkout dx-toolkit, and set DNANEXUS_HOME env var
##   9) Replace Makefile.static in UA source with one which is more suitable with this script
##   10) Build UA, and live happily ever after!

# Set appropriate number of threads for make (optional)
## UPDATE: DO NOT set MAKEFLAGS, compilation of "openssl" fail otherwise
## export MAKEFLAGS=-j`cat /proc/cpuinfo | grep processor | wc -l`

#######################################
# Install Git from source
mkdir ${HOME}/sw
cd ${HOME}/sw
# It is necessary to install the yum package "curl-devel" before installing "git", because otherwise 
# "git" fails to download from "https". See this: http://stackoverflow.com/questions/8329485/git-clone-fatal-unable-to-find-remote-helper-for-https
yum -y install curl-devel
wget http://git-core.googlecode.com/files/git-1.7.9.tar.gz
tar xvzf git-1.7.9.tar.gz
cd git-1.7.9
./configure --prefix=/usr
make
make install
#######################################

#######################################
# Install C-ares from source
cd ${HOME}/sw
wget http://c-ares.haxx.se/download/c-ares-1.9.1.tar.gz
tar xvf c-ares-1.9.1.tar.gz
cd c-ares-1.9.1
c_ares_build_location=${HOME}/sw/c-ares-1.9.1-build
./configure
make
make install
#######################################

#######################################
# Install openssl from source
cd ${HOME}/sw
wget http://www.openssl.org/source/openssl-1.0.1e.tar.gz
tar xvf openssl-1.0.1e.tar.gz
cd openssl-1.0.1e
./config -shared
make
make install # Will install it in /usr/local/ssl
#######################################

#######################################
# Install Libcurl from source (using c-ares)
export LD_LIBRARY_PATH=/usr/local/ssl/lib # So that libcurl find the correct openssl we just built
cd ${HOME}/sw
wget "http://curl.haxx.se/download/curl-7.30.0.tar.bz2"
tar -xjf curl-7.30.0.tar.bz2
mv curl-7.30.0 curl
cd curl
./configure --prefix=${HOME}/sw/local/curl_build --disable-ldap --disable-ldaps \
  --disable-rtsp --disable-dict --disable-telnet --disable-tftp --disable-pop3 \
  --disable-imap --disable-smtp --disable-gopher --disable-sspi --disable-ntlm-wb \
  --disable-tls-srp --without-gnutls --without-polarssl --without-cyassl \
  --without-nss --without-libmetalink --without-libssh2 --without-librtmp \
  --without-winidn --without-libidn --enable-ares=$cares_build_location --enable-static=yes
make
make install
#######################################

#######################################
#Install libmagic (older version do not understand "MAGIC_NO_CHECK_COMPRESS" flag, which we use in mime.cpp)
cd ${HOME}/sw
wget http://pkgs.fedoraproject.org/repo/pkgs/file/file-5.03.tar.gz/d05f08a53e5c2f51f8ee6a4758c0cc53/file-5.03.tar.gz
tar xvf file-5.03.tar.gz
cd file-5.03
./configure --prefix=/usr
make
make install # This will update the "file" version on system as well
#######################################

#######################################
# Install boost from source
cd ${HOME}/sw
############# Important hack for building boost with GCC 4.4 (instead of GCC 4.1)
## Boost "bootstrap" process ignores CC & CXX env variables, 
## So I just copied /usr/bin/gcc44 => /usr/bin/gcc , and /usr/bin/g++44 => /usr/bin/g++ (backed up the original versions)
## NOTE: This hack is also required for actually building UA
mv /usr/bin/gcc /usr/bin/gcc41-backup
mv /usr/bin/g++ /usr/bin/g++41-backup
cp /usr/bin/gcc44 /usr/bin/gcc
cp /usr/bin/g++44 /usr/bin/g++
export CC=gcc44
export CXX=g++44

wget "http://downloads.sourceforge.net/project/boost/boost/1.51.0/boost_1_51_0.tar.bz2"
bunzip2 boost_1_51_0.tar.bz2
tar xvf boost_1_51_0.tar
cd boost_1_51_0

./bootstrap.sh

# The ./b2 command should now use gcc4.4.0 to compile boost (instead of 4.1), as we moved replace "gcc/g++" with "gcc44/g++44" above.
# The output should look something like:
#	bin.v2/libs/filesystem/build/gcc-4.4.0/release/threading-multi/codecvt_error_category.o
# Note the "gcc-4.4.0" directory
./b2 toolset=gcc --link=static --threding=multi --with-chrono --with-filesystem --with-thread --with-program_options --with-system --with-regex
#######################################

#######################################
# Get dx-toolkit, and compile UA
# (note: we have already set "CC" & "CXX" env variable above,
#        and infact copied the gcc44, g++44 to gcc & g++ respectively)
mkdir ${HOME}/dev
cd ${HOME}/dev
git clone https://github.com/dnanexus/dx-toolkit.git

cd dx-toolkit
export DNANEXUS_HOME=`pwd` # Set the DNANEXUS_HOME variable which we will need for 
cd src/ua
# Write down a Makefile.static, which is applicable to these new settings
# (path to libcurl, boost, etc)
#
# Note: We use "-e" in echo, so that we write "tabs" properly in Makefile
#       (as a side effect we have to escape "\" in the script too)
# BEWARE: This will rewrite existing Makefile.static
echo -e '# -*- mode: Makefile -*-
#
# Copyright (C) 2013 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

# curl_build_dir := $(shell mktemp -d --tmpdir=/tmp curl-build-XXXXXX)

# curl:
# \t$(DNANEXUS_HOME)/src/ua/build_curl.sh $(curl_build_dir)

DXTOOLKIT_GITVERSION := $(shell git describe)

curl_dir = $(HOME)/sw/local/curl_build
cpp_dir = $(DNANEXUS_HOME)/src/cpp
dxjson_dir = $(cpp_dir)/dxjson
dxhttp_dir = $(cpp_dir)/SimpleHttpLib
dxcpp_dir = $(cpp_dir)/dxcpp
ua_dir = $(DNANEXUS_HOME)/src/ua
boost_dir=/root/sw/boost_1_51_0
ssl_dir=/usr/local/ssl
cares_lib=/root/sw/c-ares-1.9.1-build/lib

VPATH = $(dxjson_dir):$(dxhttp_dir):$(dxcpp_dir):$(ua_dir)

CFLAGS = -O3 -Wall -Wextra -pedantic

UNAME := $(shell uname)

ifeq ($(UNAME), Linux)
  CXXFLAGS = -DLINUX_BUILD=1 -D_FILE_OFFSET_BITS=64 -DUAVERSION=\\"$(VERSION)\\" -DDXTOOLKIT_GITVERSION=\\"$(DXTOOLKIT_GITVERSION)\\" -O3 -Wall -pedantic -Wextra -Werror=return-type -Wno-switch -std=c++0x -I$(curl_dir)/include -I$(cpp_dir) -I$(dxhttp_dir) -I$(dxjson_dir) -I$(dxcpp_dir) -I$(ua_dir) -I$(boost_dir)
  LDFLAGS := -static -pthread -L/usr/lib -L$(curl_dir)/lib -L$(boost_dir)/stage/lib -L$(ssl_dir)/lib -L$(cares_lib) -lcurl -lcares -lssl -lcrypto -lrt -lz -ldl -lboost_program_options -lboost_filesystem -lboost_system -lboost_thread -lboost_regex -lmagic
else ifeq ($(UNAME), Darwin)
  CXXFLAGS = -DMAC_BUILD=1 -D_FILE_OFFSET_BITS=64 -DUAVERSION=\\"$(VERSION)\\" -DDXTOOLKIT_GITVERSION=\\"$(DXTOOLKIT_GITVERSION)\\" -O3 -Wall -pedantic -Wextra -Werror=return-type -Wno-switch -std=c++0x -I$(curl_dir)/include -I$(cpp_dir) -I$(dxhttp_dir) -I$(dxjson_dir) -I$(dxcpp_dir) -I$(ua_dir)
  boost_ldir_mac = /opt/local/lib
  LDFLAGS := $(boost_ldir_mac)/libboost_program_options-mt.a $(boost_ldir_mac)/libboost_thread-mt.a $(boost_ldir_mac)/libboost_filesystem-mt.a $(boost_ldir_mac)/libboost_regex-mt.a $(curl_dir)/lib/libcurl.a /opt/local/lib/libssl.a /opt/local/lib/libcrypto.a /opt/local/lib/libcares.a /opt/local/lib/libz.a $(boost_ldir_mac)/libboost_system-mt.a /opt/local/lib/libmagic.a
else
  $(error No LDFLAGS for system $(UNAME))
endif

dxjson_objs = dxjson.o
dxhttp_objs = SimpleHttp.o SimpleHttpHeaders.o Utility.o
dxcpp_objs = api.o dxcpp.o SSLThreads.o utils.o dxlog.o
ua_objs = compress.o options.o chunk.o main.o file.o api_helper.o import_apps.o mime.o round_robin_dns.o

dxjson: $(dxjson_objs)
dxhttp: $(dxhttp_objs)
dxcpp: $(dxcpp_objs)
ua: $(ua_objs)

all: dxjson dxhttp dxcpp ua
\tg++ *.o $(LDFLAGS) -o ua
ifeq ($(UNAME), Darwin)
\tmkdir -pv resources && cp $(ua_dir)/ca-certificates.crt resources/
\tcp -v /opt/local/lib/gcc47/libstdc++.6.dylib /opt/local/lib/gcc47/libgcc_s.1.dylib .
\tinstall_name_tool -change /opt/local/lib/libstdc++.6.dylib @executable_path/libstdc++.6.dylib ua
\tinstall_name_tool -change /opt/local/lib/gcc47/libgcc_s.1.dylib @executable_path/libgcc_s.1.dylib ua
\tinstall_name_tool -change /opt/local/lib/gcc47/libgcc_s.1.dylib @executable_path/libgcc_s.1.dylib libstdc++.6.dylib
endif

dist: all
ifeq ($(UNAME), Linux)
\tmv -v ua ua-$(VERSION)-linux-old-kernel
\tbzip2 -9v ua-$(VERSION)-linux-old-kernel
else ifeq ($(UNAME), Darwin)
\tmkdir -pv ua-$(VERSION)-mac
\tmv -v ua resources/ libstdc++.6.dylib libgcc_s.1.dylib ua-$(VERSION)-mac
\ttar jcvf ua-$(VERSION)-mac.tar.bz2 ua-$(VERSION)-mac
else
\t$(error No dist recipe for system $(UNAME))
endif

clean:
\trm -v *.o ua

.PHONY: all dxjson dxhttp dxcpp ua' > Makefile.static

make # Actually make the UA
make dist # Create the .bz2 file, which we can distribute

## Reset gcc & g++ to their original version (4.1)
mv /usr/bin/gcc41-backup /usr/bin/gcc
mv /usr/bin/g++41-backup /usr/bin/g++

set +x
echo && echo "***********************"
echo "scp this file (or even better, upload with the newly compiled UA!): "$(ls `pwd`/build/*.bz2)
echo "***********************" && echo
