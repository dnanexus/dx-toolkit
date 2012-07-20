SHELL=/bin/bash -e
DESTDIR="$$DNANEXUS_HOME"

all: api_wrappers src_libs python contigset2fasta

api_wrappers:
	($(CXX) -O -Wall -o ../bin/generateWrapperTable api_wrappers/generateWrapperTable.cpp)
	../bin/generateWrapperTable ../build/list_of_all_routes.txt > ../build/wrapper_table.json
	cat ../build/wrapper_table.json | api_wrappers/generatePythonAPIWrappers.py > python/dxpy/api.py
	cat ../build/wrapper_table.json | api_wrappers/generateCppAPIHWrappers.py > cpp/dxcpp/api.h
	cat ../build/wrapper_table.json | api_wrappers/generateCppAPICCWrappers.py > cpp/dxcpp/api.cc
	cat ../build/wrapper_table.json | api_wrappers/generateJSAPIWrappers.py > javascript/DNAnexusAPI.js
	cat ../build/wrapper_table.json | api_wrappers/generatePerlAPIWrappers.py > perl/lib/DNAnexus/API.pm
	rm ../bin/generateWrapperTable

src_libs:
	mkdir -p "$(DESTDIR)/lib"
	cp -a bash perl javascript "$(DESTDIR)/lib"

python:
	mkdir -p "$(DESTDIR)/lib/python"
	(source ../environment; cd python; ./setup.py build; ./setup.py install --user --install-purelib="$(DESTDIR)/lib/python")

perl:
	(source ../environment; cd perl; dzil build; cd DNAnexus-0.1; perl Makefile.PL; make install DESTDIR="$(DESTDIR)/lib/perl" DESTINSTALLSITELIB="$(DESTDIR)/lib/perl")

contigset2fasta:
	$(MAKE) -C contigset2fasta

clean:
	$(MAKE) -C contigset2fasta clean

install:
	$(MAKE) -C contigset2fasta install

.PHONY: api_wrappers python perl contigset2fasta src_libs
