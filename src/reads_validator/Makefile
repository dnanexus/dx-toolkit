default:
	mkdir -pv build && cd build && cmake .. && $(MAKE) && mv -v reads_validator ../../../bin/dx_reads_validator

test:
	mkdir -pv build && cd build && cmake -D BUILD_TEST=TRUE .. && $(MAKE) && mv -v test_reads_validator ../tests && mv -v reads_validator ../tests

clean:
	rm -rvf build && rm -rvf ../../../bin/dx_reads_validator && rm -rvf tests/test_reads_validator && rm -rvf tests/reads_validator
