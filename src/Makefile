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

# For parallel builds, add the following to your .bashrc or other build
# environment initialization file:
#
#   export MAKEFLAGS=-j8

all: api_wrappers cpp src_libs python contigset2fasta reads_validator

# Client bindings and wrappers
# ============================

toolkit_version:
	echo "version = \"UNKNOWN_VERSION\"" > "$(DNANEXUS_HOME)/src/python/dxpy/toolkit_version.py"
	test -d "$(DNANEXUS_HOME)/.git" && echo "version = \"`git describe`\"" > "$(DNANEXUS_HOME)/src/python/dxpy/toolkit_version.py"

api_wrappers:
	($(CXX) -O -Wall -o ../bin/generateWrapperTable api_wrappers/generateWrapperTable.cpp)
	../bin/generateWrapperTable ../build/list_of_all_routes.txt > ../build/wrapper_table.json
	cat ../build/wrapper_table.json | api_wrappers/generatePythonAPIWrappers.py > python/dxpy/api.py
	cat ../build/wrapper_table.json | api_wrappers/generateCppAPIHWrappers.py > cpp/dxcpp/api.h
	cat ../build/wrapper_table.json | api_wrappers/generateCppAPICCWrappers.py > cpp/dxcpp/api.cc
	cat ../build/wrapper_table.json | api_wrappers/generateJSAPIWrappers.py > javascript/DNAnexusAPI.js
	cat ../build/wrapper_table.json | api_wrappers/generatePerlAPIWrappers.py > perl/lib/DNAnexus/API.pm
	cat ../build/wrapper_table.json | api_wrappers/generateJavaAPIWrappers.py > java/src/main/java/com/dnanexus/DXAPI.java
	touch -r "../build/list_of_all_routes.txt" python/dxpy/api.py cpp/dxcpp/api.h cpp/dxcpp/api.cc \
		javascript/DNAnexusAPI.js perl/lib/DNAnexus/API.pm java/src/main/java/com/dnanexus/DXAPI.java
	rm ../bin/generateWrapperTable

cpp: api_wrappers
	mkdir -p "$(DNANEXUS_HOME)/share/dnanexus/src"
	cp -a cpp "$(DNANEXUS_HOME)/share/dnanexus/src"

src_libs: api_wrappers
	mkdir -p "$(DNANEXUS_HOME)/share/dnanexus/lib/bash"
	install -v bash/* "$(DNANEXUS_HOME)/share/dnanexus/lib/bash"
	mkdir -p "$(DNANEXUS_HOME)/share/dnanexus/lib/javascript"
	install -v javascript/*.* "$(DNANEXUS_HOME)/share/dnanexus/lib/javascript"
	mkdir -p "$(DNANEXUS_HOME)/lib/perl5/DNAnexus"
	install -v perl/lib/DNAnexus.pm "$(DNANEXUS_HOME)/lib/perl5"
	install -v perl/lib/DNAnexus/API.pm "$(DNANEXUS_HOME)/lib/perl5/DNAnexus"

# We are going to install libraries to lib/python2.7 and then move them
# elsewhere (to share/dnanexus/lib/python2.7). Modify PYTHONPATH temporarily so
# setup.py doesn't complain.
python: api_wrappers toolkit_version
	rm -rf "$(DNANEXUS_HOME)/share/dnanexus/lib/python2.7"
	mkdir -p "$(DNANEXUS_HOME)/lib/python2.7/site-packages"
	mkdir -p "$(DNANEXUS_HOME)/share/dnanexus/lib"
	(source ../environment; export PYTHONPATH="$(DNANEXUS_HOME)/lib/python2.7/site-packages:${PYTHONPATH}"; cd python; python2.7 ./setup.py build; python2.7 ./setup.py install --prefix="$(DNANEXUS_HOME)"; mv $(DNANEXUS_HOME)/lib/python2.7 $(DNANEXUS_HOME)/share/dnanexus/lib)

# TODO: fix up perl and java targets

perl: api_wrappers
	(source ../environment; cd perl; dzil build; cd DNAnexus-0.1; perl Makefile.PL; make install DESTDIR="$(DNANEXUS_HOME)/lib/perl" DESTINSTALLSITELIB="$(DNANEXUS_HOME)/lib/perl")

java: api_wrappers
	mkdir -p "$(DNANEXUS_HOME)/lib/java"
	(cd java; mvn compile assembly:single)
	cp -f java/target/*.jar "$(DNANEXUS_HOME)/lib/java"

# Tools
# =====

contigset2fasta: api_wrappers
	$(MAKE) -C contigset2fasta install DESTDIR=$(DNANEXUS_HOME) PREFIX=/

reads_validator: api_wrappers
	$(MAKE) -C reads_validator install DESTDIR=$(DNANEXUS_HOME) PREFIX=/

ua:
	$(MAKE) -C ua install DESTDIR=$(DNANEXUS_HOME) PREFIX=/

# Tests
# =====

test: test_python test_perl

test_python:
	for i in python/test/*; do $$i; done

test_perl:
	(cd perl; prove)

# Installation
# ============

base_install: all
	mkdir -p $(DESTDIR)/$(PREFIX)/bin
	rm -rf $(DESTDIR)/$(PREFIX)/lib/python2.7/site-packages/dxpy*              # Prevent file-overwriting-dir (or the opposite) problem

	cp -a $(DNANEXUS_HOME)/bin/{dx*,contigset2fasta} $(DESTDIR)/$(PREFIX)/bin/ # binaries
	cp -a $(DNANEXUS_HOME)/{lib,share} $(DESTDIR)/$(PREFIX)                    # libraries
	cp -a $(DNANEXUS_HOME)/doc $(DESTDIR)/$(PREFIX)/share/dnanexus             # docs

	rm -f $(DESTDIR)/$(PREFIX)/share/dnanexus/src/cpp/*/.gitignore

# Many apps have an autodep that says DESTDIR=/ PREFIX=/opt/dnanexus. Work
# around that and install environment in the correct place until all those apps
# have been replaced.
install: base_install
	(if [ "$(DESTDIR)" == "/" ]; then install -v $(DNANEXUS_HOME)/environment $(PREFIX); else install -v $(DNANEXUS_HOME)/environment $(DESTDIR); fi)

debian_install: base_install
	mkdir -p $(DESTDIR)/etc/profile.d                                        # Install environment file into etc
	install -v $(DNANEXUS_HOME)/environment $(DESTDIR)/etc/profile.d/dnanexus.environment
	virtualenv $(DESTDIR)/$(PREFIX)/share/dnanexus                           # Install dxpy into lib/python/dist-packages, deps under share/dnanexus
	mkdir -p $(DESTDIR)/$(PREFIX)/lib/python2.7/dist-packages
	cp -a $(DNANEXUS_HOME)/share/dnanexus/lib/python2.7/site-packages/dxpy* $(DESTDIR)/$(PREFIX)/lib/python2.7/dist-packages
	ls $(DNANEXUS_HOME)/share/dnanexus/lib/python2.7/site-packages | grep dxpy > $(DESTDIR)/$(PREFIX)/lib/python2.7/dist-packages/dxpy.pth
	$(DESTDIR)/$(PREFIX)/share/dnanexus/bin/pip install -r python/requirements.txt
	rm -rf $(DESTDIR)/$(PREFIX)/share/dnanexus/bin                           # Remove residual stuff left by pip/virtualenv
	rm -f $(DESTDIR)/$(PREFIX)/share/dnanexus/local/*

# Documentation
# =============

doc: doc_python doc_dxcpp doc_dxjson doc_java

setup_doc_virtualenv: build/doc_env/.setup_virtualenv

build/doc_env/.setup_virtualenv: ../build/doc_build_requirements.txt
	@if ! which pip > /dev/null; then echo "pip not found, please run apt-get install python-pip"; exit 1; fi
	@if ! which virtualenv > /dev/null; then echo "virtualenv not found, please run apt-get install python-virtualenv"; exit 1; fi
	virtualenv ../build/doc_env
	(unset PYTHONPATH; source ../build/doc_env/bin/activate; pip install --requirement=../build/doc_build_requirements.txt)
	virtualenv --relocatable ../build/doc_env
	touch ../build/doc_env/.setup_virtualenv

doc_python: python setup_doc_virtualenv
	source ../build/doc_env/bin/activate; source "../environment"; export PYTHONPATH="$${PYTHONPATH}:../lib"; $(MAKE) -C python/doc html

doc_dxcpp:
	mkdir -p ../doc/cpp/dxcpp
	doxygen cpp/dxcpp/Doxyfile

doc_dxjson:
	mkdir -p ../doc/cpp/dxjson
	doxygen cpp/dxjson/Doxyfile

doc_java:
	mkdir -p ../doc/java
	(cd java; mvn compile javadoc:javadoc)

# Clean
# =====

distclean: clean

# TODO: clean up files in /bin, too
clean:
	$(MAKE) -C contigset2fasta clean
	-rm -f ../bin/contigset2fasta
	$(MAKE) -C reads_validator clean
	-rm -f ../bin/dx-reads-validator
	$(MAKE) -C ua clean
	-rm -rf python/build
	-rm -rf java/target
	-rm -f ../bin/pyrsa-*
	-rm -f ../build/wrapper_table.json
	-rm -rf ../lib
	-rm -rf ../share

.PHONY: all toolkit_version api_wrappers src_libs python perl java contigset2fasta reads_validator ua test test_python test_perl install base_install debian_install doc setup_doc_build_virtualenv doc_python doc_dxcpp doc_dxjson doc_java
