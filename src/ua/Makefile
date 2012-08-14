all:
	mkdir -pv build
	cd build && GITVERSION=$(shell git describe) cmake ..
	make -C build

clean:
	rm -rvf build

install: all
	cp -v build/ua $(DNANEXUS_HOME)/bin

.PHONY: all
