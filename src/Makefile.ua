# Copyright (C) 2013-2016 DNAnexus, Inc.
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


include mk/config.mk
include mk/Makefile.${PLATFORM}

# Global variables
UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu

ua: print_vars ua_deps
	echo ">>>> Running  $@ <<<<"
	$(MAKE) -C ua install dist DESTDIR=$(DESTDIR) PREFIX=$(PREFIX)

print_vars:
	echo "#############################################################"
	echo ">>>> Running  $@ <<<<"
	echo "MAKE=${MAKE}"
	echo "MAKEOPTS=${MAKEOPTS}"
	echo "UNAME=${UNAME}"
	echo "OS=${OS}"
	echo "PLATFORM=${PLATFORM}"
	echo "DNANEXUS_HOME=${DNANEXUS_HOME}"
	echo "DNANEXUS_LIBDIR=${DNANEXUS_LIBDIR}"
	echo "PATH=${PATH}"
	echo "DESTDIR=${DESTDIR}"
	echo "PREFIX=${PREFIX}"
	echo "CENTOS_MAJOR_VERSION=${CENTOS_MAJOR_VERSION}"
	echo "FEDORA_MAJOR_VERSION=${FEDORA_MAJOR_VERSION}"
	echo "UBUNTU_VERSION=${UBUNTU_VERSION}"
	echo "RHEL_MAJOR_VERSION=${RHEL_MAJOR_VERSION}"
	echo "#############################################################"

# Upload Agent dependencies
# =========================
ua_deps: openssl boost curl file/stage/lib/libmagic.la

# OpenSSL
OPENSSL_PV=3.4.1
OPENSSL_SHA=d3469baf41823a28ad71aae12b2fbb9fe3b19a0d
openssl: print_vars shasum
	echo ">>>> Running $@ <<<<"
	mkdir -p openssl
	curl ${UBUNTU_MIRROR}/pool/main/o/openssl/openssl_${OPENSSL_PV}.orig.tar.gz > openssl-${OPENSSL_PV}.tar.gz
	[[ $$(shasum openssl-${OPENSSL_PV}.tar.gz|cut -f 1 -d ' ') == $(OPENSSL_SHA) ]]
	tar -xzf openssl-${OPENSSL_PV}.tar.gz -C openssl --strip-components=1
ifeq ($(UNAME), Darwin)
	cd openssl; ./Configure darwin64-x86_64-cc -shared --openssldir="$$(pwd)/stage"
else
	echo ">>>> $@ - Configureing... <<<<"
	cd openssl; ./Configure --prefix=$(PREFIX) --openssldir=$(PREFIX)
endif
	echo ">>>> $@ - Installing... <<<<"
	$(MAKE) -C openssl -j1 install

# Boost
BOOST_LIBS=all
BOOST_MAJOR=1
BOOST_MINOR=87
BOOST_PV=$(BOOST_MAJOR).$(BOOST_MINOR)
BOOST_URL=https://archives.boost.io/release/${BOOST_PV}.0/source/boost_${BOOST_MAJOR}_${BOOST_MINOR}_0.tar.bz2
BOOST_SHA=acf19e9d53f0b6db8e85452978eb9c731ef8b865

# See https://svn.boost.org/trac/boost/ticket/5680 for Windows/MinGW details
boost: print_vars shasum
	echo ">>>> Running $@ <<<<"
	curl -L ${BOOST_URL} > boost-$(BOOST_PV).tar.bz2
	[[ $$(shasum boost-$(BOOST_PV).tar.bz2|cut -f 1 -d ' ') == $(BOOST_SHA) ]]
	mkdir -p boost
	tar -xjf boost-$(BOOST_PV).tar.bz2 -C boost --strip-components=1
	echo ">>>> $@ - Installing... <<<<"
ifeq ($(OS), Windows_NT)
	echo ">>>> $@ - Building for Windows <<<<"
	cd boost; ./bootstrap.sh --with-toolset=mingw --with-libraries=${BOOST_LIBS}
	sed -i -e s/mingw/gcc/ boost/project-config.jam
	cd boost; ./b2 --build-type=complete --layout=tagged -j8 stage
else ifeq ($(UNAME)$(CC), Darwinclang)
	echo ">>>> $@ - Building for Macosx <<<<"
	cd boost; ./bootstrap.sh --with-libraries=${BOOST_LIBS} --with-toolset=clang
	cd boost; ./b2 --layout=tagged -j8 stage cxxflags="-stdlib=libc++" linkflags="-stdlib=libc++" toolset=clang
else
	echo ">>>> $@ - Building for Linux <<<<"
	cd boost; ./bootstrap.sh --with-libraries=${BOOST_LIBS}
	cd boost; ./b2 --prefix=$(PREFIX) --layout=tagged -j8 stage cxxflags="-std=c++14 -stdlib=libc++"
endif

# curl
curl: c-ares libcurl

# c-ares
# https://github.com/c-ares/c-ares/releases/download/v1.34.5/c-ares-1.34.5.tar.gz
CARES_PV=1.34.5
CARES_SHA=2ae693c5be7e79240003c98e3becde2d3a65f8fb
c-ares: print_vars shasum
	echo ">>>> Running $@ <<<<"
	mkdir -p c-ares
	curl -L https://github.com/c-ares/c-ares/releases/download/v${CARES_PV}/c-ares-${CARES_PV}.tar.gz > c-ares-${CARES_PV}.tar.gz
	[[ $$(shasum c-ares-${CARES_PV}.tar.gz|cut -f 1 -d ' ') == $(CARES_SHA) ]]
	tar -xzf c-ares-${CARES_PV}.tar.gz -C c-ares --strip-components=1
	echo ">>>> $@ - Configureing... <<<<"
	cd c-ares; ./configure --prefix=$(PREFIX) --enable-static=no --enable-shared=yes
	echo ">>>> $@ - Installing... <<<<"
	$(MAKE) -C c-ares install

# libcurl
CURL_MAJOR=8
CURL_MINOR=13
CURL_MICRO=0
CURL_PV=$(CURL_MAJOR).$(CURL_MINOR).$(CURL_MICRO)
CURL_PV_UNDERSCORES=$(CURL_MAJOR)_$(CURL_MINOR)_$(CURL_MICRO)
CURL_MIRROR=https://github.com/bagder/curl/releases/download
CURL_URL=$(CURL_MIRROR)/curl-$(CURL_PV_UNDERSCORES)/curl-$(CURL_PV).tar.gz
CURL_SHA=07d4eae21a532e6b9736009bcb96140e6f03abc7
ifeq ($(OS), Windows_NT)
	CURL_WITH_SSL=--with-winssl
	CURL_SHARED_LIBS=--enable-shared=yes
else ifeq ($(UNAME), Darwin)
	CURL_WITH_SSL=--with-ssl=$(DNANEXUS_HOME)/src/openssl
	CURL_SHARED_LIBS=--enable-shared=yes
else ifeq ($(UNAME), Linux)
	CURL_WITH_SSL=--with-openssl=$(DESTDIR)$(PREFIX)
	CURL_SHARED_LIBS=--enable-shared=yes
else
	CURL_WITH_SSL=--with-ssl
	CURL_SHARED_LIBS=--enable-shared=no
endif

libcurl: print_vars shasum openssl
	echo ">>>> Running  $@ <<<<"
	mkdir -p curl
	curl -L $(CURL_URL) > curl-$(CURL_PV).tar.gz
	[[ $$(shasum curl-$(CURL_PV).tar.gz|cut -f 1 -d ' ') == $(CURL_SHA) ]]
	tar -xzf curl-$(CURL_PV).tar.gz -C curl --strip-components=1
	echo ">>>> $@ - Configureing... <<<<"
# TODO: the path for --enable-ares is hardcoded, should be fixed later after we succussfully build everything.
	cd curl; ./configure --disable-ldap --disable-ldaps \
	    --disable-rtsp --disable-dict --disable-telnet --disable-tftp --disable-pop3 \
	    --disable-imap --disable-smtp --disable-gopher --disable-sspi \
	    --disable-tls-srp --without-gnutls --without-libssh2 --without-librtmp \
	    --without-winidn --enable-ares=$(DESTDIR)$(prefix) \
	    --disable-static --enable-shared $(CURL_WITH_SSL)
	echo ">>>> $@ - Installing... <<<<"
	$(MAKE) -C curl install

# what is this file lib?
FILE_PV=5.32
FILE_SHA=6a84bddca6fa9bcf6882832ccece38cf22b9b600
file/stage/lib/libmagic.la: shasum
	echo ">>>> Running  $@ <<<<"
	mkdir -p file
	curl ${UBUNTU_MIRROR}/pool/main/f/file/file_${FILE_PV}.orig.tar.xz > file-${FILE_PV}.tar.xz
	[[ $$(shasum file-${FILE_PV}.tar.xz|cut -f 1 -d ' ') == $(FILE_SHA) ]]
	tar -xf file-${FILE_PV}.tar.xz -C file --strip-components=1
	echo ">>>> $@ - Configureing... <<<<"
	cd file; ./configure --enable-static=no --enable-shared=yes --prefix=$(PREFIX)
	echo ">>>> $@ - Installing... <<<<"
	$(MAKE) -C file install DESTDIR="$(DNANEXUS_HOME)src/file/stage"

# Clean
# =====

distclean: clean

clean:
	$(MAKE) -C dx-verify-file clean
	$(MAKE) -C ua clean
	-find ../bin -type f \( -name dx -or -name 'dx-*' \) -not -name 'dx-unpack*' -not -name 'dx-su-*' -delete
	-rm -f ../bin/docker2aci ../bin/proot
	-rm -rf python/{build,*.{egg,egg-info}}
	-rm -rf java/target
	-rm -rf ../lib
	-rm -rf ../share
	-rm -f ../build/info/version
	-rm -rf "$(DX_PY_ENV)"ma
	-rm -rf boost c-ares curl file openssl


# shasum
shasum:
	if ! hash shasum; then cd "${DNANEXUS_HOME}/build/bin"; ln -s "$$(command -v sha1sum)" shasum; fi

.PHONY: all toolkit_version api_wrappers src_libs python java ua test test_python install base_install debian_install doc setup_doc_build_virtualenv doc_python doc_dxcpp doc_dxjson doc_java R doc_R boost boost_build install_sysdeps