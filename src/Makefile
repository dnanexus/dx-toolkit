SHELL=/bin/bash -e
DESTDIR="$$DNANEXUS_HOME"

all: src_libs python contigset2fasta

src_libs:
	mkdir -p "$(DESTDIR)/lib"
	cp -a bash perl javascript "$(DESTDIR)/lib"

python:
	mkdir -p "$(DESTDIR)/lib/python"
	(source ../environment; cd python; ./setup.py build; ./setup.py install --install-purelib="$(DESTDIR)/lib/python")

contigset2fasta:
	$(MAKE) -C contigset2fasta

clean:
	$(MAKE) -C contigset2fasta clean

install:
	$(MAKE) -C contigset2fasta install

.PHONY: python contigset2fasta src_libs
