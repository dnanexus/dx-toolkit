#!/bin/sh -ex

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