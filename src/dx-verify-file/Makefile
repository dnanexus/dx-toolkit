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

ifndef DESTDIR
	export DESTDIR=/opt/dnanexus
endif
ifndef PREFIX
	export PREFIX=/
endif
UNAME := $(shell uname)

all:
	mkdir -pv build
ifeq ($(OS), Windows_NT)
	cd build && cmake -G "MSYS Makefiles" .. -DCMAKE_BUILD_TYPE:STRING=RELEASE -DSTATIC_BOOST=1
else ifeq ($(UNAME), Darwin)
	# Add paths to homebrew openssl dir to cmake cache
	cd build && cmake .. -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl -DOPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include -DOPENSSL_LIBRARIES=/usr/local/opt/openssl/lib -DCMAKE_BUILD_TYPE:STRING=RELEASE -DSTATIC_BOOST=1 -DCMAKE_EXE_LINKER_FLAGS="-lpthread"
else
	cd build && cmake .. -DCMAKE_BUILD_TYPE:STRING=RELEASE -DSTATIC_BOOST=1 -DCMAKE_EXE_LINKER_FLAGS="-lpthread"
endif
	$(MAKE) -C build

clean:
	rm -rvf build/ build_static/

install: all
	install -v build/dx-verify-file $(DESTDIR)/$(PREFIX)/bin

build_static:
	mkdir -pv build_static
ifeq ($(UNAME), MINGW32_NT-6.2)
	make -C build_static -f ../Makefile.mw all
else
	make -C build_static -f ../Makefile.static all
endif

dist_static: build_static
ifeq ($(UNAME), MINGW32_NT-6.2)
	make -C build_static -f ../Makefile.mw dist
else
	make -C build_static -f ../Makefile.static dist
endif

.PHONY: all clean install build_static dist_static
