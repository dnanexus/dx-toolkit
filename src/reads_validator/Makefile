default:
	mkdir -pv build && cd build && cmake .. && $(MAKE)

test:
	mkdir -pv build && cd build && cmake -D BUILD_TEST=TRUE .. && $(MAKE) && mv -v test_reads_validator ../tests && mv -v reads_validator ../tests

clean:
	rm -rvf build

install: default
	cp -v build/reads_validator $(DNANEXUS_HOME)/bin/dx-reads-validator
