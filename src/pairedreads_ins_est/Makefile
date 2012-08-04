all: agile lastz self

agile:
	mkdir -pv build
	(cd build; wget http://users.eecs.northwestern.edu/~smi539/agile/agile_x86_64_0.4.0.tar.gz -O agile.tar.gz; tar -xzf agile.tar.gz)

lastz:
	mkdir -pv build
	(cd build; wget http://www.bx.psu.edu/miller_lab/dist/lastz-1.02.00.tar.gz -O lastz.tar.gz; tar -xzf lastz.tar.gz; cd lastz-distrib-1.02.00/src; scp ../../../lastz.makefile Makefile; make)

self:
	mkdir -pv build
	(cd build; cmake ../; make)
clean:
	rm -rvf build
	rm -rvf $(DNANEXUS_HOME)/bin/agile
	rm -rvf $(DNANEXUS_HOME)/bin/lastz
	rm -rvf $(DNANEXUS_HOME)/bin/pairedreads_ins_est

install:
	cp -v build/agile_x86_64_0.4.0/agile $(DNANEXUS_HOME)/bin/
	cp -v build/lastz-distrib-1.02.00/src/lastz $(DNANEXUS_HOME)/bin/
	cp -v build/pairedreads_ins_est $(DNANEXUS_HOME)/bin/

.PHONY: all
