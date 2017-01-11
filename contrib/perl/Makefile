SHELL=/bin/bash -e

all: lib/DNAnexus/API.pm
	(source ../../environment; dzil build; cd DNAnexus-0.0.1; perl Makefile.PL; make install DESTDIR="$(DNANEXUS_HOME)/lib/perl5" DESTINSTALLSITELIB="$(DNANEXUS_HOME)/lib/perl5")

lib/DNAnexus/API.pm: ../../src/api_wrappers/wrapper_table.json generatePerlAPIWrappers.py
	cat ../../src/api_wrappers/wrapper_table.json | ./generatePerlAPIWrappers.py > lib/DNAnexus/API.pm
