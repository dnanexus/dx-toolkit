# Copyright (C) 2013-2014 DNAnexus, Inc.
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

SHELL=/bin/bash -e

DNANEXUS_HOME = $(CURDIR)/../..

VERSION := $(shell cat $(DNANEXUS_HOME)/build/info/ua_version || echo 'UNKNOWN_VERSION')
DXTOOLKIT_GITVERSION := $(shell cat $(DNANEXUS_HOME)/build/info/version || echo 'UNKNOWN_VERSION')
CENTOS_MAJOR_VERSION := $(shell grep -o "CentOS release [0-9]\+" /etc/issue | sed -e "s/CentOS release //")

curl_dir = $(DNANEXUS_HOME)/src/curl/stage
cares_dir = $(DNANEXUS_HOME)/src/c-ares/stage
libmagic_dir = $(DNANEXUS_HOME)/src/file/stage
boost_dir = $(DNANEXUS_HOME)/src/boost
openssl_dir = $(DNANEXUS_HOME)/src/openssl

cpp_dir = $(DNANEXUS_HOME)/src/cpp
dxjson_dir = $(cpp_dir)/dxjson
dxhttp_dir = $(cpp_dir)/SimpleHttpLib
dxcpp_dir = $(cpp_dir)/dxcpp
ua_dir = $(DNANEXUS_HOME)/src/ua

VPATH = $(dxjson_dir):$(dxhttp_dir):$(dxcpp_dir):$(ua_dir)

# TODO: -DBOOST_THREAD_USE_LIB

CFLAGS = -O3 -Wall -Wextra -Werror=return-type -Wno-switch -pedantic
CXXFLAGS = $(CFLAGS)
CXXFLAGS += -D_FILE_OFFSET_BITS=64 -DUAVERSION=\"$(VERSION)\" -DDXTOOLKIT_GITVERSION=\"$(DXTOOLKIT_GITVERSION)\"
CXXFLAGS += -I$(libmagic_dir)/include -I$(openssl_dir)/include -I$(curl_dir)/include -I$(cares_dir)/include -I$(boost_dir) -I$(cpp_dir) -I$(dxhttp_dir) -I$(dxjson_dir) -I$(dxcpp_dir) -I$(ua_dir)
ifeq ($(CENTOS_MAJOR_VERSION), 5)
	CXXFLAGS += -DOLD_KERNEL_SUPPORT=1
endif

LDFLAGS = -L$(boost_dir)/stage/lib -L$(curl_dir)/lib -L$(cares_dir)/lib -L$(libmagic_dir)/lib -L$(openssl_dir)

UNAME := $(shell uname)

# Determine path to OS X dyldinfo util; as of 10.9 it's no longer in /usr/bin
# If it's not in PATH, set it to the usual default location:
ifeq ($(UNAME), Darwin)
	DYLDINFO_PATH := $(shell type -p dyldinfo)
	ifndef DYLDINFO_PATH
		DYLDINFO_PATH := /Library/Developer/CommandLineTools/usr/bin/dyldinfo
	endif
endif

# We use the -std=gnu++0x flag instead of c++0x for Windows compilation, because
# c++0x mode turns on the __STRICT_ANSI__ flag, which forbids functions such as fseeko64() (which we use)
ifeq ($(OS), Windows_NT)
	CXXFLAGS += -DWINDOWS_BUILD=1 -std=gnu++0x
	LDFLAGS += -lws2_32 -lregex -lgnurx.dll -lshlwapi
	LDFLAGS += -lcurl.dll -lcares.dll -lboost_program_options-mt.dll -lboost_filesystem-mt.dll -lboost_system-mt.dll -lboost_thread-mt.dll -lboost_regex-mt.dll
else
	LDFLAGS += -pthread -lcurl -lcares -lboost_program_options-mt -lboost_filesystem-mt -lboost_system-mt -lboost_thread-mt -lboost_regex-mt

	ifeq ($(UNAME), Darwin)
		CXXFLAGS += -DMAC_BUILD=1 -std=c++0x
		LDFLAGS += -Wl,-headerpad,100
		ifeq ($(CC), clang)
			CXXFLAGS += -stdlib=libc++
		endif
	else
		CXXFLAGS += -DLINUX_BUILD=1 -std=c++0x
		LDFLAGS += -lrt -lidn -static
	endif
endif

LDFLAGS += -lssl -lcrypto -lz -ldl -lmagic -lm
ifeq ($(UNAME)$(CC), Darwinclang)
	LDFLAGS += -lc++ -lc++abi
else
	LDFLAGS += -lstdc++
endif

dxjson_objs = dxjson.o
dxhttp_objs = SimpleHttp.o SimpleHttpHeaders.o Utility.o
dxcpp_objs = api.o dxcpp.o SSLThreads.o utils.o dxlog.o
ua_objs = compress.o options.o chunk.o main.o file.o api_helper.o import_apps.o mime.o round_robin_dns.o common_utils.o

all: ua

ua: $(dxjson_objs) $(dxhttp_objs) $(dxcpp_objs) $(ua_objs)
	$(CXX) *.o $(LDFLAGS) -o ua

D = dnanexus-upload-agent-$(VERSION)
dist: all
	rm -rf dist $(D)-*
	mkdir -p dist
ifeq ($(OS), Windows_NT)
	install ../file/magic/magic.mgc dist/
	install {$(curl_dir),$(cares_dir),$(libmagic_dir),$(openssl_dir)/stage}/bin/*.dll $(boost_dir)/stage/lib/*-mt.dll dist/
	for lib in zlib1 libgcc_s_dw2-1 libstdc++-6 libgnurx-0; do \
	    install "$$(which $$lib.dll)" dist/; \
	done
	install -s ua.exe dist/
	cp -a dist $(D)-windows
	zip -r $(D)-windows.zip $(D)-windows
else ifeq ($(UNAME), Linux)
	install -s ua dist/
	cp -a dist $(D)-linux
	tar -czf $(D)-linux.tar.gz $(D)-linux
else ifeq ($(UNAME), Darwin)
	cp -af {$(curl_dir)/lib,$(cares_dir)/lib,$(libmagic_dir)/lib,$(boost_dir)/stage/lib,$(openssl_dir)}/*.dylib dist/
	mkdir -p dist/resources && install ca-certificates.crt dist/resources/
	install -s ua dist/
ifeq ($(CC), clang)
	install $$($(DYLDINFO_PATH) -dylibs ua|grep libc++) dist/
	install_name_tool -change /usr/lib/libc++.1.dylib "@loader_path/libc++.1.dylib" dist/ua
	install_name_tool -change /usr/lib/libc++abi.dylib "@loader_path/libc++abi.dylib" dist/ua
else
	install $$($(DYLDINFO_PATH) -dylibs ua|grep libstdc++) dist/
	install $$($(DYLDINFO_PATH) -dylibs ua|grep libgcc) dist/
endif
	for bin in dist/ua dist/*.dylib; do \
	    for lib in $$($(DYLDINFO_PATH) -dylibs $$bin|egrep -v '(for arch|attributes|/usr/lib)'); do \
	        install_name_tool -change $$lib "@loader_path/$$(basename $$lib)" $$bin; \
	    done; \
	done
	cp -a dist $(D)-osx
	zip -r $(D)-osx.zip $(D)-osx
else
	$(error No dist recipe for system $(UNAME))
endif

installer: dist
ifeq ($(OS), Windows_NT)
	export VERSIONMAJOR=$$(echo $(VERSION) | cut -d . -f 1); \
	export VERSIONMINOR=$$(echo $(VERSION) | cut -d . -f 2); \
	export VERSIONBUILD=$$(echo $(VERSION) | cut -d . -f 3); \
	cd installers/NSIS; "$${PROGRAMFILES}/NSIS/makensis" ua.nsi
endif

install: dist
	cp -a dist/* $(DESTDIR)/$(PREFIX)/bin/

clean:
	rm -rf *.o ua ua.exe dist $(D)*

.PHONY: all clean dist install installer
