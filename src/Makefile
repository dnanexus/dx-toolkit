SHELL=/bin/bash -e

ifndef DNANEXUS_HOME
	export DNANEXUS_HOME=$(CURDIR)/..
endif

ifndef DESTDIR
	export DESTDIR=$(DNANEXUS_HOME)
endif

all: api_wrappers src_libs python contigset2fasta reads_validator

api_wrappers:
	($(CXX) -O -Wall -o ../bin/generateWrapperTable api_wrappers/generateWrapperTable.cpp)
	../bin/generateWrapperTable ../build/list_of_all_routes.txt > ../build/wrapper_table.json
	cat ../build/wrapper_table.json | api_wrappers/generatePythonAPIWrappers.py > python/dxpy/api.py
	cat ../build/wrapper_table.json | api_wrappers/generateCppAPIHWrappers.py > cpp/dxcpp/api.h
	cat ../build/wrapper_table.json | api_wrappers/generateCppAPICCWrappers.py > cpp/dxcpp/api.cc
	cat ../build/wrapper_table.json | api_wrappers/generateJSAPIWrappers.py > javascript/DNAnexusAPI.js
	cat ../build/wrapper_table.json | api_wrappers/generatePerlAPIWrappers.py > perl/lib/DNAnexus/API.pm
	touch -r "../build/list_of_all_routes.txt" python/dxpy/api.py cpp/dxcpp/api.h cpp/dxcpp/api.cc \
		javascript/DNAnexusAPI.js perl/lib/DNAnexus/API.pm
	rm ../bin/generateWrapperTable

src_libs:
	mkdir -p "$(DNANEXUS_HOME)/lib"
	cp -a bash perl javascript "$(DNANEXUS_HOME)/lib"

python:
	mkdir -p "$(DNANEXUS_HOME)/lib/python2.7/site-packages"
	(source ../environment; cd python; ./setup.py build; ./setup.py install --prefix="$(DNANEXUS_HOME)")

perl:
	(source ../environment; cd perl; dzil build; cd DNAnexus-0.1; perl Makefile.PL; make install DESTDIR="$(DNANEXUS_HOME)/lib/perl" DESTINSTALLSITELIB="$(DNANEXUS_HOME)/lib/perl")

java:
	(cd java; ant)

test: test_python test_perl

test_python:
	for i in python/test/*; do $$i; done

test_perl:
	(cd perl; prove)

contigset2fasta:
	$(MAKE) -C contigset2fasta

reads_validator:
	$(MAKE) -C reads_validator

clean:
	$(MAKE) -C contigset2fasta clean
	$(MAKE) -C reads_validator clean

install: api_wrappers src_libs python
	$(MAKE) -C contigset2fasta install
	$(MAKE) -C reads_validator install
	mkdir -p $(DESTDIR)
	if [[ $(DNANEXUS_HOME) != $(DESTDIR) ]]; then \
		cp -a $(DNANEXUS_HOME)/* $(DESTDIR); \
	fi

setup_doc_virtualenv: build/doc_env/.setup_virtualenv

build/doc_env/.setup_virtualenv: ../build/doc_build_requirements.txt
	@if ! which pip > /dev/null; then echo "pip not found, please run apt-get install python-pip"; exit 1; fi
	@if ! which virtualenv > /dev/null; then echo "virtualenv not found, please run apt-get install python-virtualenv"; exit 1; fi
	virtualenv ../build/doc_env
	(unset PYTHONPATH; source ../build/doc_env/bin/activate; pip install --requirement=../build/doc_build_requirements.txt)
	virtualenv --relocatable ../build/doc_env
	touch ../build/doc_env/.setup_virtualenv

doc: doc_python doc_dxcpp doc_dxjson

doc_python: python setup_doc_virtualenv
	source ../build/doc_env/bin/activate; source "../environment"; export PYTHONPATH="$${PYTHONPATH}:../lib"; $(MAKE) -C python/doc html

doc_dxcpp:
	mkdir -p ../doc/cpp/dxcpp
	doxygen cpp/dxcpp/Doxyfile

doc_dxjson:
	mkdir -p ../doc/cpp/dxjson
	doxygen cpp/dxjson/Doxyfile

.PHONY: api_wrappers python perl contigset2fasta reads_validator src_libs test test_python test_perl setup_doc_build_virtualenv doc doc_python doc_dxcpp doc_dxjson
