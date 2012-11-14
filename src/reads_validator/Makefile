ifndef DESTDIR
	export DESTDIR=/opt/dnanexus
endif
ifndef PREFIX
	export PREFIX=/
endif

all:
	mkdir -pv build
	cd build && cmake ..
	$(MAKE) -C build

test:
	mkdir -pv build
	cd build && cmake -D BUILD_TEST=TRUE .. && $(MAKE) && mv -v test_reads_validator ../tests && mv -v reads_validator ../tests

clean:
	rm -rvf build

install: all
	install -v build/reads_validator $(DESTDIR)/$(PREFIX)/bin/dx-reads-validator

.PHONY: all test clean install
