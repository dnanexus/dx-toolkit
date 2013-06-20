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

SHELL=/bin/bash -e

# If installing into the system directories you probably want to set
#   DESTDIR=/ PREFIX=/usr
ifndef DESTDIR
	export DESTDIR=/opt/dnanexus
endif
ifndef PREFIX
	export PREFIX=/
endif

export DNANEXUS_HOME=$(CURDIR)/..
export DNANEXUS_PY27_ENV=$(DNANEXUS_HOME)/build/py27_env

# For parallel builds, add the following to your .bashrc or other build
# environment initialization file:
#
#   export MAKEFLAGS=-j8

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
	    echo "version = \"UNKNOWN_VERSION\"" > "$(DNANEXUS_HOME)/src/python/dxpy/toolkit_version.py"; \
	    echo -e "module DX\n  VERSION = \"0.0.0\"\nend\n" > "$(DNANEXUS_HOME)/src/ruby/lib/dxruby/version.rb"; \
	    echo UNKNOWN_VERSION > "$(DNANEXUS_HOME)/build/info/version"; \
	fi)
	(if [ -d "$(DNANEXUS_HOME)/.git" ]; then \
	    echo "version = \"`git describe --always`\"" > "$(DNANEXUS_HOME)/src/python/dxpy/toolkit_version.py"; \
	    git describe --always | perl -ne '/^v(\d+.\d+.\d+)/ and print "module DX\n  VERSION = \"$$1\"\nend\n"' > "$(DNANEXUS_HOME)/src/ruby/lib/dxruby/version.rb"; \
	    git describe --always > "$(DNANEXUS_HOME)/build/info/version"; \
	fi)

python/dxpy/api.py: api_wrappers/wrapper_table.json api_wrappers/generatePythonAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generatePythonAPIWrappers.py > python/dxpy/api.py

cpp/dxcpp/api.h: api_wrappers/wrapper_table.json api_wrappers/generateCppAPIHWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateCppAPIHWrappers.py > cpp/dxcpp/api.h

cpp/dxcpp/api.cc: api_wrappers/wrapper_table.json api_wrappers/generateCppAPICCWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateCppAPICCWrappers.py > cpp/dxcpp/api.cc

javascript/DNAnexusAPI.js: api_wrappers/wrapper_table.json api_wrappers/generateJSAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateJSAPIWrappers.py > javascript/DNAnexusAPI.js

perl/lib/DNAnexus/API.pm: api_wrappers/wrapper_table.json api_wrappers/generatePerlAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generatePerlAPIWrappers.py > perl/lib/DNAnexus/API.pm

java/src/main/java/com/dnanexus/DXAPI.java: api_wrappers/wrapper_table.json api_wrappers/generateJavaAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateJavaAPIWrappers.py > java/src/main/java/com/dnanexus/DXAPI.java

R/dxR/R/api.R: api_wrappers/wrapper_table.json api_wrappers/generateRAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateRAPIWrappers.py > R/dxR/R/api.R

ruby/lib/dxruby/api.rb: api_wrappers/wrapper_table.json api_wrappers/generateRubyAPIWrappers.py
	cat api_wrappers/wrapper_table.json | api_wrappers/generateRubyAPIWrappers.py > ruby/lib/dxruby/api.rb

api_wrappers: python/dxpy/api.py cpp/dxcpp/api.h cpp/dxcpp/api.cc javascript/DNAnexusAPI.js perl/lib/DNAnexus/API.pm java/src/main/java/com/dnanexus/DXAPI.java R/dxR/R/api.R ruby/lib/dxruby/api.rb

cpp: api_wrappers
	mkdir -p "$(DNANEXUS_HOME)/share/dnanexus/src"
	cp -a cpp "$(DNANEXUS_HOME)/share/dnanexus/src"

src_libs: api_wrappers
	$(eval DNANEXUS_LIBDIR := "$(DNANEXUS_HOME)/share/dnanexus/lib")
	mkdir -p "$(DNANEXUS_LIBDIR)"
	cp -R -v bash javascript "$(DNANEXUS_LIBDIR)"
	mkdir -p "$(DNANEXUS_HOME)/lib/perl5"
	cp -R -v perl/lib/* "$(DNANEXUS_HOME)/lib/perl5/"

python_deps: $(DNANEXUS_PY27_ENV)/lib

$(DNANEXUS_PY27_ENV)/lib: python/requirements.txt python/requirements_dxfs.txt
	rm -rf "$(DNANEXUS_PY27_ENV)"
	virtualenv --python=python2.7 --distribute "$(DNANEXUS_PY27_ENV)"
	virtualenv --relocatable "$(DNANEXUS_PY27_ENV)"
	(source "$(DNANEXUS_PY27_ENV)/bin/activate"; pip install --ignore-installed -r python/requirements.txt -r python/requirements_dxfs.txt)

# We are going to install libraries to lib/python2.7 and then move them
# elsewhere (to share/dnanexus/lib/python2.7). Modify PYTHONPATH temporarily so
# setup.py doesn't complain.
python: api_wrappers toolkit_version python_deps
	rm -rf "$(DNANEXUS_HOME)/share/dnanexus/lib/python2.7"
	mkdir -p "$(DNANEXUS_HOME)/lib/python2.7/site-packages"
	mkdir -p "$(DNANEXUS_HOME)/share/dnanexus/lib"
	cp -a "$(DNANEXUS_PY27_ENV)/lib/python2.7/site-packages" "$(DNANEXUS_HOME)/lib/python2.7/"
	cp -a "$(DNANEXUS_PY27_ENV)/bin/"*python-argcomplete* "$(DNANEXUS_HOME)/bin/"
	(source ../environment; export PYTHONPATH="$(DNANEXUS_HOME)/lib/python2.7/site-packages:${PYTHONPATH}"; cd python; python2.7 ./setup.py build; python2.7 ./setup.py install --prefix="$(DNANEXUS_HOME)" --install-scripts="$(DNANEXUS_HOME)/bin"; mv $(DNANEXUS_HOME)/lib/python2.7 $(DNANEXUS_HOME)/share/dnanexus/lib)
	rm -f ../bin/easy_install*

# TODO: fix up perl and java targets

perl: api_wrappers
	(source ../environment; cd perl; dzil build; cd DNAnexus-0.1; perl Makefile.PL; make install DESTDIR="$(DNANEXUS_HOME)/lib/perl5" DESTINSTALLSITELIB="$(DNANEXUS_HOME)/lib/perl5")

# If running in fakeroot, override some maven settings so that maven can still
# find a localRepository to write to.
java: api_wrappers
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

ua:
	$(MAKE) -C ua install DESTDIR=$(DNANEXUS_HOME) PREFIX=/

# Tests
# =====

test: test_python test_perl

test_ruby: ruby
	(source ../environment && ruby ruby/test/test_dxruby.rb)

coverage_python:
	(cd python; export PYTHONPATH="$$PYTHONPATH:$$(echo $$(pwd)/coverage-*.egg)"; \
	 test/coveragereport.py --strip-prefix="$$(readlink -f $$(pwd))/build/lib.linux-x86_64-2.7" --cover dxpy --cover scripts --exclude dxpy.packages --exclude EGG-INFO.scripts --exclude test --exclude setuptools --path-alias "$$(readlink -f $$(pwd)/../../share/dnanexus/lib/python2.7/site-packages/dxpy*.egg)"="$$(readlink -f $$(pwd))" --path-alias "$$(readlink -f $$(pwd)/build/lib.linux-x86_64-2.7)"="$$(readlink -f $$(pwd))")

test_python:
	(echo "import coverage; coverage.process_startup()" > ../share/dnanexus/lib/python2.7/site-packages/sitecustomize.py)
	(cd python; rm -vf .coverage build/*/.coverage*)
	(cd python; export COVERAGE_PROCESS_START="$$(pwd)/.coveragerc"; ./setup.py nosetests --cover-inclusive)
	(cd python; mv build/*/.coverage* .)
	(cd python; coverage combine)
	(rm -f ../share/dnanexus/lib/python2.7/site-packages/sitecustomize.py)

test_perl:
	(cd perl; prove)

test_java:
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
	rm -f $(DESTDIR)/$(PREFIX)/share/dnanexus/src/cpp/*/.gitignore
	cp -a $(DNANEXUS_HOME)/doc $(DESTDIR)/$(PREFIX)/share/dnanexus             # docs

	../build/fix_shebang_lines.sh $(DESTDIR)/$(PREFIX)/bin --debian-system-install

	mkdir -p $(DESTDIR)/etc/profile.d                                        # Install environment file into etc
	install -v -m0644 $(DNANEXUS_HOME)/environment $(DESTDIR)/etc/profile.d/dnanexus.environment
	virtualenv $(DESTDIR)/$(PREFIX)/share/dnanexus                           # Install dxpy into lib/python/dist-packages, deps under share/dnanexus
	mkdir -p $(DESTDIR)/$(PREFIX)/lib/python2.7/dist-packages
	cp -a $(DNANEXUS_HOME)/share/dnanexus/lib/python2.7/site-packages/dxpy* $(DESTDIR)/$(PREFIX)/lib/python2.7/dist-packages
	ls $(DNANEXUS_HOME)/share/dnanexus/lib/python2.7/site-packages | grep dxpy > $(DESTDIR)/$(PREFIX)/lib/python2.7/dist-packages/dxpy.pth
	$(DESTDIR)/$(PREFIX)/share/dnanexus/bin/pip install -r python/requirements.txt -r python/requirements_dxfs.txt
	rm -rf $(DESTDIR)/$(PREFIX)/share/dnanexus/bin                           # Remove residual stuff left by pip/virtualenv
	rm -f $(DESTDIR)/$(PREFIX)/share/dnanexus/local/*

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
	rm -rf ../doc/python/html/_modules
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

# Bundled utilities
# =================

jq: git_submodules
	(cd jq; autoreconf --install)
	(cd jq; ./configure --disable-docs)
	$(MAKE) -C jq -j PATH=$$(brew --prefix bison || echo .)/bin:$$PATH
	cp -f jq/jq ../bin/

# Clean
# =====

distclean: clean

# TODO: clean up files in /bin, too
clean:
	$(MAKE) -C dx-contigset-to-fasta clean
	$(MAKE) -C dx-wig-to-wiggle clean
	-rm -f ../bin/dx-contigset-to-fasta
	-rm -f ../bin/contigset2fasta
	-rm -rf python/{build,*.{egg,egg-info}}
	-rm -rf java/target
	-rm -rf ../lib
	-rm -rf ../share
	-rm -rf "$(DNANEXUS_PY27_ENV)"

.PHONY: all toolkit_version api_wrappers src_libs python_deps python perl java ruby dx-contigset-to-fasta ua test test_ruby test_python test_perl install base_install debian_install doc setup_doc_build_virtualenv doc_python doc_dxcpp doc_dxjson doc_java R doc_R doc_ruby
