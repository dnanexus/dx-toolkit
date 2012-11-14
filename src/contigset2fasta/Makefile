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

clean:
	rm -rvf build

install: all
	install -v build/contigset2fasta $(DESTDIR)/$(PREFIX)/bin

.PHONY: all clean install
