#!/bin/sh -ex
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

# This script builds a custom libcurl. Many of the protocols that are
# enabled by default are disabled to limit dependencies. Some dependencies
# are replaced to prevent other issues. For example, the default NSS-based
# name resolution is replaced with the c-ares library to fix a
# thread-safety issue.

build_dir=$1
cd $build_dir

pwd

wget "http://curl.haxx.se/download/curl-7.27.0.tar.bz2"
tar jxvf curl-7.27.0.tar.bz2
ln -s curl-7.27.0 curl
cd curl

./configure --prefix=${HOME}/sw/local/curl --disable-ldap --disable-ldaps \
  --disable-rtsp --disable-dict --disable-telnet --disable-tftp --disable-pop3 \
  --disable-imap --disable-smtp --disable-gopher --disable-sspi --disable-ntlm-wb \
  --disable-tls-srp --without-gnutls --without-polarssl --without-cyassl \
  --without-nss --without-libmetalink --without-libssh2 --without-librtmp \
  --without-winidn --without-libidn --without-ssl --with-winssl --disable-ares

make
make install
