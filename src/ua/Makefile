all:
	mkdir -pv build
	make -C build -f ../Makefile.static all

clean:
	rm -rvf build

dist: all
	make -C build -f ../Makefile.static dist

install: all
	cp -v build/ua $(DNANEXUS_HOME)/bin

.PHONY: all
