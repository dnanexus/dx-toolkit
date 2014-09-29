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

ifndef MAKEOPTS
	ifneq ($(OS), Windows_NT)
		MAKEOPTS=-j -l 2.0
	endif
endif
MAKE:=$(MAKE) $(MAKEOPTS)

UNAME := $(shell uname)

CENTOS_MAJOR_VERSION := $(shell test -e /etc/issue && (grep -o "CentOS release [0-9]\+" /etc/issue | sed -e "s/CentOS release //"))
FEDORA_MAJOR_VERSION := $(shell test -e /etc/issue && (grep -o "Fedora release [0-9]\+" /etc/issue | sed -e "s/Fedora release //"))
UBUNTU_VERSION := $(shell test -e /etc/issue && (grep -o "Ubuntu [0-9]\+\.[0-9]\+" /etc/issue | sed -e "s/Ubuntu //"))

ifndef CCACHE_DISABLE
	export PATH := /usr/lib/ccache:$(PATH)
endif

# If installing into the system directories you probably want to set
#   DESTDIR=/ PREFIX=/usr
ifndef DESTDIR
	export DESTDIR=/opt/dnanexus
endif
ifndef PREFIX
	export PREFIX=/
endif

export DNANEXUS_HOME=$(CURDIR)/..
export PATH := $(DNANEXUS_HOME)/build/bin:$(PATH)
export DX_PY_ENV=$(DNANEXUS_HOME)/build/py27_env

# Short-circuit sudo when running as root. In a chrooted environment we are
# likely to be running as root already, and sudo may not be present on minimal
# installations.
ifeq (${USER}, root)
	MAYBE_SUDO=
else
	MAYBE_SUDO='sudo'
endif

all: api_wrappers cpp src_libs python dx-contigset-to-fasta dx-verify-file dx-wig-to-wiggle jq

# Don't try to initialize the submodule in source distributions, it should
# already be there and ready to build.
git_submodules:
	(if [ -d "$(DNANEXUS_HOME)/.git" ]; then \
	    cd ..; \
	    git submodule init; \
	    "`which git-retry || which git`" submodule update; \
	fi)

# Client bindings and wrappers
# ============================

toolkit_version:
	(if [ ! -e "$(DNANEXUS_HOME)/src/python/dxpy/toolkit_version.py" ]; then \
	    echo "version = 'UNKNOWN_VERSION'" > "$(DNANEXUS_HOME)/src/python/dxpy/toolkit_version.py"; \
	    echo -e "module DX\n  VERSION = \"0.0.0\"\nend\n" > "$(DNANEXUS_HOME)/src/ruby/lib/dxruby/version.rb"; \
	    echo -e "package com.dnanexus;\nclass DXToolkitVersion {\n    public static final String TOOLKIT_VERSION = \"0.0.0\";\n}" > "$(DNANEXUS_HOME)/src/java/src/main/java/com/dnanexus/DXToolkitVersion.java"; \
	    echo UNKNOWN_VERSION > "$(DNANEXUS_HOME)/build/info/version"; \
	fi)
	(if [ -d "$(DNANEXUS_HOME)/.git" ]; then \
	    VERSION=$$(git describe --always|sed 's/-/+/'); \
	    echo "version = '$${VERSION/#v/}'" > "$(DNANEXUS_HOME)/src/python/dxpy/toolkit_version.py"; \
	    echo $$VERSION | perl -ne '/^v(\d+.\d+.\d+)/ and print "module DX\n  VERSION = \"$$1\"\nend\n"' > "$(DNANEXUS_HOME)/src/ruby/lib/dxruby/version.rb"; \
	    echo -e "package com.dnanexus;\nclass DXToolkitVersion {\n    public static final String TOOLKIT_VERSION = \"$$VERSION\";\n}" > "$(DNANEXUS_HOME)/src/java/src/main/java/com/dnanexus/DXToolkitVersion.java"; \
	    echo $$VERSION > "$(DNANEXUS_HOME)/build/info/version"; \
	fi)

python/dxpy/api.py: api_wrappers/wrapper_table.json api_wrappers/generatePythonAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generatePythonAPIWrappers.py > python/dxpy/api.py

cpp/dxcpp/api.h: api_wrappers/wrapper_table.json api_wrappers/generateCppAPIHWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateCppAPIHWrappers.py > cpp/dxcpp/api.h

cpp/dxcpp/api.cc: api_wrappers/wrapper_table.json api_wrappers/generateCppAPICCWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateCppAPICCWrappers.py > cpp/dxcpp/api.cc

perl/lib/DNAnexus/API.pm: api_wrappers/wrapper_table.json api_wrappers/generatePerlAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generatePerlAPIWrappers.py > perl/lib/DNAnexus/API.pm

java/src/main/java/com/dnanexus/DXAPI.java: api_wrappers/wrapper_table.json api_wrappers/generateJavaAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateJavaAPIWrappers.py > java/src/main/java/com/dnanexus/DXAPI.java

R/dxR/R/api.R: api_wrappers/wrapper_table.json api_wrappers/generateRAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateRAPIWrappers.py > R/dxR/R/api.R

ruby/lib/dxruby/api.rb: api_wrappers/wrapper_table.json api_wrappers/generateRubyAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateRubyAPIWrappers.py > ruby/lib/dxruby/api.rb

api_wrappers: toolkit_version python/dxpy/api.py cpp/dxcpp/api.h cpp/dxcpp/api.cc perl/lib/DNAnexus/API.pm java/src/main/java/com/dnanexus/DXAPI.java R/dxR/R/api.R ruby/lib/dxruby/api.rb

cpp: api_wrappers
	mkdir -p "$(DNANEXUS_HOME)/share/dnanexus/src"
	cp -a cpp "$(DNANEXUS_HOME)/share/dnanexus/src"

src_libs: api_wrappers
	$(eval DNANEXUS_LIBDIR := "$(DNANEXUS_HOME)/share/dnanexus/lib")
	mkdir -p "$(DNANEXUS_LIBDIR)"
	cp -R -v bash "$(DNANEXUS_LIBDIR)"
	mkdir -p "$(DNANEXUS_HOME)/lib/perl5"
	cp -R -v perl/lib/* "$(DNANEXUS_HOME)/lib/perl5/"

python_wheels: ../build/wheels

../build/wheels: python/requirements*.txt
	pip wheel -r python/requirements.txt -r python/requirements_test.txt -r python/requirements_setuptools.txt --wheel-dir ../build/wheels
# xattr < 0.7 fails to install using Python 3; that's OK since it's only needed for dxfs, which doesn't work on Python 3 either.
	-pip wheel -r python/requirements_dxfs.txt --wheel-dir ../build/wheels
ifeq ($(UNAME), Darwin)
	pip wheel -r python/requirements_readline.txt
endif

python_deps: $(DX_PY_ENV)/lib

$(DX_PY_ENV)/lib: python/requirements*.txt
	python -c 'import sys; exit("dx-toolkit is not compatible with Python < 2.7" if sys.version_info < (2, 7) else 0)'
	rm -rf "$(DX_PY_ENV)"
	virtualenv --python=python "$(DX_PY_ENV)"
	virtualenv --relocatable "$(DX_PY_ENV)"
	(source "$(DX_PY_ENV)/bin/activate"; pip install --upgrade --find-links="file://$(DNANEXUS_HOME)/build/wheels" --install-option="--install-lib=$(DX_PY_ENV)/lib/python2.7/site-packages" -r python/requirements_setuptools.txt)
	(source "$(DX_PY_ENV)/bin/activate"; pip install --ignore-installed --use-wheel --find-links="file://$(DNANEXUS_HOME)/build/wheels" --install-option="--install-lib=$(DX_PY_ENV)/lib/python2.7/site-packages" -r python/requirements.txt -r python/requirements_dxfs.txt)
ifeq ($(UNAME), Darwin)
	(source "$(DX_PY_ENV)/bin/activate"; pip install --ignore-installed --use-wheel --find-links="file://$(DNANEXUS_HOME)/build/wheels" --install-option="--install-lib=$(DX_PY_ENV)/lib/python2.7/site-packages" -r python/requirements_readline.txt)
endif
ifdef DNANEXUS_INSTALL_PYTHON_TEST_DEPS
	(source "$(DX_PY_ENV)/bin/activate"; pip install --ignore-installed --use-wheel --find-links="file://$(DNANEXUS_HOME)/build/wheels" --install-option="--install-lib=$(DX_PY_ENV)/lib/python2.7/site-packages" -r python/requirements_test.txt)
endif

# We are going to install libraries to lib/python2.7 and then move them
# elsewhere (to share/dnanexus/lib/python2.7). Modify PYTHONPATH temporarily so
# setup.py doesn't complain.
python: api_wrappers toolkit_version python_deps
	rm -rf "$(DNANEXUS_HOME)/share/dnanexus/lib/python2.7"
	mkdir -p "$(DNANEXUS_HOME)/lib/python2.7/site-packages"
	mkdir -p "$(DNANEXUS_HOME)/share/dnanexus/lib"
	cp -a "$(DX_PY_ENV)/lib/python2.7/site-packages" "$(DNANEXUS_HOME)/lib/python2.7/"
	cp -a "$(DX_PY_ENV)/bin/"*python-argcomplete* "$(DNANEXUS_HOME)/bin/"
ifdef DNANEXUS_INSTALL_PYTHON_TEST_DEPS
	cp -af "$(DX_PY_ENV)"/bin/coverage* "$(DNANEXUS_HOME)/bin/"
endif
	(source ../environment; source "$(DX_PY_ENV)/bin/activate"; export PYTHONPATH="$(DNANEXUS_HOME)/lib/python2.7/site-packages:${PYTHONPATH}"; cd python; python ./setup.py build; python ./setup.py install --install-lib="$(DNANEXUS_HOME)/lib/python2.7/site-packages" --install-scripts="$(DNANEXUS_HOME)/bin"; mv $(DNANEXUS_HOME)/lib/python2.7 $(DNANEXUS_HOME)/share/dnanexus/lib)
	rm -f ../bin/easy_install* ../bin/xattr

# TODO: fix up perl and java targets

perl: api_wrappers
	(source ../environment; cd perl; dzil build; cd DNAnexus-0.1; perl Makefile.PL; make install DESTDIR="$(DNANEXUS_HOME)/lib/perl5" DESTINSTALLSITELIB="$(DNANEXUS_HOME)/lib/perl5")

# If running in fakeroot, override some maven settings so that maven can still
# find a localRepository to write to.
java: api_wrappers toolkit_version
	mkdir -p "$(DNANEXUS_HOME)/lib/java"
	(cd java; if [ "$(FAKED_MODE)" == "" ]; then mvn compile assembly:single; else mvn -s m2-settings.xml compile assembly:single; fi)
	cp -f java/target/*.jar "$(DNANEXUS_HOME)/lib/java"

R: api_wrappers toolkit_version
	mkdir -p "$(DNANEXUS_HOME)/lib/R"
	(cd R; export R_LIBS_SITE="$(DNANEXUS_HOME)/lib/R"; R -q --no-restore --no-save < install_dxR_build_deps.R; R -q --no-restore --no-save -e "library(roxygen2); roxygenize(\"dxR\")"; R CMD INSTALL --build dxR --library="$(DNANEXUS_HOME)/lib/R"; )

ruby: api_wrappers toolkit_version
	(cd ruby; gem build dxruby.gemspec)
	(cd ruby; export GEM_HOME="$(DNANEXUS_HOME)/lib/rubygems"; gem install dxruby*.gem)

# Tools
# =====

dx-contigset-to-fasta: api_wrappers
	$(MAKE) -C dx-contigset-to-fasta install DESTDIR=$(DNANEXUS_HOME) PREFIX=/

dx-verify-file: api_wrappers
	$(MAKE) -C dx-verify-file install DESTDIR=$(DNANEXUS_HOME) PREFIX=/

dx-wig-to-wiggle: api_wrappers
	$(MAKE) -C dx-wig-to-wiggle install DESTDIR=$(DNANEXUS_HOME) PREFIX=/

ua: toolkit_version ua_deps
	$(MAKE) -C ua install dist DESTDIR=$(DNANEXUS_HOME) PREFIX=/

# Tests
# =====

test: test_python test_perl

test_ruby: ruby
	(source ../environment && ruby ruby/test/test_dxruby.rb)

coverage_python:
	(cd python; export PYTHONPATH="$$PYTHONPATH:$$(echo $$(pwd)/coverage-*.egg)"; \
	 test/coveragereport.py --strip-prefix="$$(readlink -f $$(pwd))/build/lib.linux-x86_64-2.7" --cover dxpy. --cover scripts --exclude dxpy.packages --exclude EGG-INFO.scripts --exclude test. --exclude setuptools --path-alias "$$(readlink -f $$(pwd)/../../share/dnanexus/lib/python2.7/site-packages/dxpy*.egg)"="$$(readlink -f $$(pwd))" --path-alias "$$(readlink -f $$(pwd)/build/lib.linux-x86_64-2.7)"="$$(readlink -f $$(pwd))")

test_python:
	../build/run_python_integration_tests.py

test_perl: perl
	(cd perl; prove)

# mvn will compile the Java files, but it doesn't know how to generate the API
# wrappers and toolkit release file.
test_java: java
	(cd java; mvn test)

# Installation
# ============

base_install: all
	mkdir -p $(DESTDIR)/$(PREFIX)/bin
	rm -rf $(DESTDIR)/$(PREFIX)/lib/python2.7/site-packages/dxpy*              # Prevent file-overwriting-dir (or the opposite) problem

	cp -a $(DNANEXUS_HOME)/bin/* $(DESTDIR)/$(PREFIX)/bin/                     # binaries

# Many apps have an autodep that says DESTDIR=/ PREFIX=/opt/dnanexus. Work
# around that and install environment in the correct place until all those apps
# have been replaced.
install: base_install
	cp -a $(DNANEXUS_HOME)/{lib,share} $(DESTDIR)/$(PREFIX)                    # libraries
	rm -f $(DESTDIR)/$(PREFIX)/share/dnanexus/src/cpp/*/.gitignore
	cp -a $(DNANEXUS_HOME)/doc $(DESTDIR)/$(PREFIX)/share/dnanexus             # docs
	(if [ "$(DESTDIR)" == "/" ]; then install -v -m0644 $(DNANEXUS_HOME)/environment $(PREFIX); else install -v -m0644 $(DNANEXUS_HOME)/environment $(DESTDIR); fi)

debian_install: base_install
	cp -a $(DNANEXUS_HOME)/share $(DESTDIR)/$(PREFIX)                          # core libraries
	mkdir -p $(DESTDIR)/$(PREFIX)/share/dnanexus/build
	cp -a $(DNANEXUS_HOME)/build/info $(DESTDIR)/$(PREFIX)/share/dnanexus/build/
	rm -f $(DESTDIR)/$(PREFIX)/share/dnanexus/src/cpp/*/.gitignore
	cp -a $(DNANEXUS_HOME)/doc $(DESTDIR)/$(PREFIX)/share/dnanexus             # docs

	../build/fix_shebang_lines.sh $(DESTDIR)/$(PREFIX)/bin --debian-system-install

	mkdir -p $(DESTDIR)/etc/profile.d                                        # Install environment file into etc
	install -v -m0644 $(DNANEXUS_HOME)/environment $(DESTDIR)/etc/profile.d/dnanexus.environment
	cp -a $(DNANEXUS_HOME)/share/dnanexus/lib/python2.7/site-packages/* $(DESTDIR)/$(PREFIX)/share/dnanexus/lib/python2.7/site-packages/
	ls $(DNANEXUS_HOME)/share/dnanexus/lib/python2.7/site-packages | grep dxpy > $(DESTDIR)/$(PREFIX)/share/dnanexus/lib/python2.7/site-packages/dxpy.pth

debian_java_install: java
	mkdir -p $(DESTDIR)/$(PREFIX)/share/java
	$(eval CLEANED_JAR_NAME := `cd "$(DNANEXUS_HOME)"/lib/java; ls *.jar | sed "s/dnanexus-api-\([0-9]\+\.[0-9]\+\.[0-9]\+\)-.*/dnanexus-api-\1.jar/g"`)
	(cd "$(DNANEXUS_HOME)"/lib/java; cp -a *.jar $(DESTDIR)/$(PREFIX)/share/java/"$(CLEANED_JAR_NAME)")

debian_perl_install: perl
	mkdir -p $(DESTDIR)/$(PREFIX)/lib
	cp -a $(DNANEXUS_HOME)/lib/perl5 $(DESTDIR)/$(PREFIX)/lib

debian_r_install: R
	mkdir -p $(DESTDIR)/$(PREFIX)/lib/R/site-library
	cp -a $(DNANEXUS_HOME)/lib/R/{RCurl,RJSONIO} $(DESTDIR)/$(PREFIX)/lib/R/site-library/
	cp -a $(DNANEXUS_HOME)/lib/R/dxR $(DESTDIR)/$(PREFIX)/lib/R/site-library/

debian_ruby_install: ruby
	mkdir -p $(DESTDIR)/$(PREFIX)/share/dnanexus/lib
	cp -a "$(DNANEXUS_HOME)"/lib/rubygems $(DESTDIR)/$(PREFIX)/share/dnanexus/lib

# Documentation
# =============

doc: doc_python doc_dxcpp doc_dxjson doc_java

setup_doc_virtualenv: .setup_doc_virtualenv

.setup_doc_virtualenv: ../build/doc_build_requirements.txt
	@if ! which pip > /dev/null; then echo "pip not found, please run apt-get install python-pip"; exit 1; fi
	@if ! which virtualenv > /dev/null; then echo "virtualenv not found, please run apt-get install python-virtualenv"; exit 1; fi
	(unset PYTHONPATH; virtualenv ../build/doc_env; source ../build/doc_env/bin/activate; pip install --find-links=http://dnanexus-pypi2.s3.amazonaws.com/index.html --requirement=../build/doc_build_requirements.txt)
	virtualenv --relocatable ../build/doc_env
	touch .setup_doc_virtualenv

doc_python: python setup_doc_virtualenv
	rm -rf ../doc/python/*
	source ../build/doc_env/bin/activate; source "../environment"; export PYTHONPATH="$${PYTHONPATH}:../lib"; $(MAKE) -C python/doc html

doc_dxcpp:
	mkdir -p ../doc/cpp/dxcpp
	doxygen cpp/dxcpp/Doxyfile

doc_dxjson:
	mkdir -p ../doc/cpp/dxjson
	doxygen cpp/dxjson/Doxyfile

doc_java: api_wrappers
	mkdir -p ../doc/java
	(cd java; mvn compile javadoc:javadoc)

doc_R: R
	(export R_LIBS_SITE="$(DNANEXUS_HOME)/lib/R"; R --no-save < "$(DNANEXUS_HOME)/src/R/install_dxR_doc_deps.R")
	(export R_LIBS_SITE="$(DNANEXUS_HOME)/lib/R"; R -q -e "library('parser'); library('highlight'); library('staticdocs'); build_package('R/dxR', '$(DNANEXUS_HOME)/doc/R/')")

doc_ruby: ruby
	-rm -rf ../doc/ruby
	(cd ruby; yard --output-dir "$(DNANEXUS_HOME)/doc/ruby")

# Upload Agent dependencies
# =========================
ua_deps: openssl boost curl file/stage/lib/libmagic.la

UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu

shasum:
	if ! hash shasum; then cd "${DNANEXUS_HOME}/build/bin"; ln -s "$$(command -v sha1sum)" shasum; fi

git: shasum
	if ! hash git; then $(MAKE) build_git; fi

GIT_PV=1.8.4
GIT_SHA=6b3924a5e7a05893670ca9744b7f5166cf03193b
build_git:
	mkdir -p git
	curl -L "https://github.com/git/git/archive/v$(GIT_PV).tar.gz" > git-$(GIT_PV).tar.gz
	[[ $$(shasum git-$(GIT_PV).tar.gz|cut -f 1 -d ' ') == $(GIT_SHA) ]]
	tar -xzf git-$(GIT_PV).tar.gz -C git --strip-components=1
	(cd git && autoreconf --install && ./configure && $(MAKE) && $(MAKE) install)
	(cd ../build/bin && ln -s ../../src/git/git)

OPENSSL_PV=1.0.1e
OPENSSL_SHA=3f1b1223c9e8189bfe4e186d86449775bd903460
openssl: shasum
	if [[ "$(OS)" == Windows_NT || "$(UNAME)" == Darwin || "$(CENTOS_MAJOR_VERSION)" == "5" ]]; then \
	    $(MAKE) build_openssl; \
	fi

build_openssl: openssl/libssl.a

openssl/libssl.a:
	mkdir -p openssl
	curl ${UBUNTU_MIRROR}/pool/main/o/openssl/openssl_${OPENSSL_PV}.orig.tar.gz > openssl-${OPENSSL_PV}.tar.gz
	[[ $$(shasum openssl-${OPENSSL_PV}.tar.gz|cut -f 1 -d ' ') == $(OPENSSL_SHA) ]]
	tar -xzf openssl-${OPENSSL_PV}.tar.gz -C openssl --strip-components=1
ifeq ($(UNAME), Darwin)
	cd openssl; ./Configure darwin64-x86_64-cc -shared --openssldir="$$(pwd)/stage"
else
	cd openssl; ./config shared --openssldir="$$(pwd)/stage"
endif
	$(MAKE) -C openssl -j1 install

BOOST_PV=1.55
BOOST_SHA=cef9a0cc7084b1d639e06cd3bc34e4251524c840
BOOST_LIBS=filesystem,program_options,regex,system,thread,atomic,chrono,date_time,exception,iostreams,signals
boost: boost/stage

# See https://svn.boost.org/trac/boost/ticket/5680 for Windows/MinGW details
boost/stage: shasum
	mkdir -p boost
	curl ${UBUNTU_MIRROR}/pool/universe/b/boost${BOOST_PV}/boost${BOOST_PV}_${BOOST_PV}.0.orig.tar.bz2 > boost-$(BOOST_PV).tar.bz2
	[[ $$(shasum boost-$(BOOST_PV).tar.bz2|cut -f 1 -d ' ') == $(BOOST_SHA) ]]
	tar -xjf boost-$(BOOST_PV).tar.bz2 -C boost --strip-components=1
ifeq ($(OS), Windows_NT)
	cd boost; ./bootstrap.sh --with-toolset=mingw --with-libraries=${BOOST_LIBS}
	sed -i -e s/mingw/gcc/ boost/project-config.jam
	cd boost; ./b2 --build-type=complete --layout=tagged -j8 stage
else ifeq ($(CENTOS_MAJOR_VERSION), 5)
	echo 'using gcc : 4.4 : g++44 ;' >> boost/tools/build/v2/user-config.jam
	$(MAKE) boost_build
else
	$(MAKE) boost_build
endif

boost_build:
	cd boost; ./bootstrap.sh --with-libraries=${BOOST_LIBS}
	cd boost; ./b2 --layout=tagged -j8 stage

curl: c-ares/stage/lib/libcares.la curl/stage/lib/libcurl.la

CURL_PV=7.32.0
CURL_SHA=cadc5648e4cd4ea750a8c580311722ed1fb1573a
ifeq ($(OS), Windows_NT)
	CURL_WITH_SSL=--with-winssl
	CURL_SHARED_LIBS=--enable-shared=yes
else ifeq ($(UNAME), Darwin)
	CURL_WITH_SSL=--with-ssl=$$(pwd)/../openssl/stage
	CURL_SHARED_LIBS=--enable-shared=yes
else ifeq ($(CENTOS_MAJOR_VERSION), 5)
	CURL_WITH_SSL=--with-ssl=$$(pwd)/../openssl/stage
	CURL_SHARED_LIBS=--enable-shared=no
else
	CURL_WITH_SSL=--with-ssl
	CURL_SHARED_LIBS=--enable-shared=no
endif
curl/stage/lib/libcurl.la: shasum openssl
	mkdir -p curl
	curl ${UBUNTU_MIRROR}/pool/main/c/curl/curl_${CURL_PV}.orig.tar.gz > curl-${CURL_PV}.tar.gz
	[[ $$(shasum curl-${CURL_PV}.tar.gz|cut -f 1 -d ' ') == $(CURL_SHA) ]]
	tar -xzf curl-${CURL_PV}.tar.gz -C curl --strip-components=1
	cd curl; ./configure --disable-ldap --disable-ldaps \
	    --disable-rtsp --disable-dict --disable-telnet --disable-tftp --disable-pop3 \
	    --disable-imap --disable-smtp --disable-gopher --disable-sspi --disable-ntlm-wb \
	    --disable-tls-srp --without-gnutls --without-polarssl --without-cyassl \
	    --without-nss --without-libmetalink --without-libssh2 --without-librtmp \
	    --without-winidn --without-libidn --enable-ares="$(DNANEXUS_HOME)/src/c-ares/stage" \
	    --enable-static=yes $(CURL_SHARED_LIBS) $(CURL_WITH_SSL) --prefix=/
	$(MAKE) -C curl install DESTDIR="$(DNANEXUS_HOME)/src/curl/stage"

CARES_PV=1.10.0
CARES_SHA=e44e6575d5af99cb3a38461486e1ee8b49810eb5
c-ares/stage/lib/libcares.la: shasum
	mkdir -p c-ares
	curl ${UBUNTU_MIRROR}/pool/main/c/c-ares/c-ares_${CARES_PV}.orig.tar.gz > c-ares-${CARES_PV}.tar.gz
	[[ $$(shasum c-ares-${CARES_PV}.tar.gz|cut -f 1 -d ' ') == $(CARES_SHA) ]]
	tar -xzf c-ares-${CARES_PV}.tar.gz -C c-ares --strip-components=1
	cd c-ares; ./configure --prefix=/
	$(MAKE) -C c-ares install DESTDIR="$(DNANEXUS_HOME)/src/c-ares/stage"

FILE_PV=5.09
FILE_SHA=9d905f9e50033c3f5be3728473cbb709a41550fb
file/stage/lib/libmagic.la: shasum
	mkdir -p file
	curl ${UBUNTU_MIRROR}/pool/main/f/file/file_${FILE_PV}.orig.tar.gz > file-${FILE_PV}.tar.gz
	[[ $$(shasum file-${FILE_PV}.tar.gz|cut -f 1 -d ' ') == $(FILE_SHA) ]]
	tar -xzf file-${FILE_PV}.tar.gz -C file --strip-components=1
	cd file; ./configure --prefix=/
	$(MAKE) -C file install DESTDIR="$(DNANEXUS_HOME)/src/file/stage"

# Bundled utilities
# =================

jq: git_submodules ../bin/jq

../bin/jq: jq/
	(cd jq; autoreconf --install)
	(if [[ "$(UNAME)" == Darwin ]] && ! (gcc --version | grep -q LLVM); then export LDFLAGS="$$LDFLAGS -lgcc_eh"; fi; \
	 cd jq; ./configure --disable-docs)
	$(MAKE) -C jq -j PATH=$$(brew --prefix bison || echo .)/bin:$$PATH
	cp -f jq/jq ../bin/

# System dependencies
# ===================

install_sysdeps:
ifeq ($(CENTOS_MAJOR_VERSION), 5)
	${MAYBE_SUDO} yum groupinstall -y "Development tools"
	${MAYBE_SUDO} yum install -y cmake gcc44-c++ zlib-devel bzip2-devel curl-devel
else ifeq ($(FEDORA_MAJOR_VERSION), 20)
	${MAYBE_SUDO} yum groupinstall -y "Development tools"
	${MAYBE_SUDO} yum install -y gcc gcc-c++ automake bison flex python python-pip python-virtualenv boost-devel boost-static cmake openssl-devel libcurl-devel bzip2-devel
else ifeq ($(UBUNTU_VERSION), 12.04)
	${MAYBE_SUDO} apt-get install make python-setuptools python-pip python-virtualenv python-dev \
		g++ cmake libcurl4-openssl-dev zlib1g-dev libbz2-dev flex bison autoconf
else ifeq ($(UBUNTU_VERSION), 14.04)
	${MAYBE_SUDO} apt-get install make python-setuptools python-pip python-virtualenv python-dev \
		g++ cmake libcurl4-openssl-dev zlib1g-dev libbz2-dev flex bison autoconf
endif

# Clean
# =====

distclean: clean

clean:
	$(MAKE) -C dx-contigset-to-fasta clean
	$(MAKE) -C dx-wig-to-wiggle clean
	$(MAKE) -C dx-verify-file clean
	$(MAKE) -C ua clean
	-find ../bin -type f \( -name jq -or -name dx -or -name '*argcomplete*' -or -name 'dx-*' \) -not -name 'dx-unpack*' -delete
	-rm -rf python/{build,*.{egg,egg-info}}
	-rm -rf java/target
	-rm -rf ../lib
	-rm -rf ../share
	-rm ../build/info/version
	-rm -rf "$(DX_PY_ENV)"
	-rm -rf boost c-ares curl file openssl
	-$(MAKE) -C jq clean

.PHONY: all toolkit_version api_wrappers src_libs python_deps python perl java ruby dx-contigset-to-fasta ua test test_ruby test_python test_perl install base_install debian_install doc setup_doc_build_virtualenv doc_python doc_dxcpp doc_dxjson doc_java R doc_R doc_ruby boost boost_build install_sysdeps
