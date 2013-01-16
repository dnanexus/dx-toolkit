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
	install -v build/dx-contigset-to-fasta $(DESTDIR)/$(PREFIX)/bin
	ln -sf ./dx-contigset-to-fasta $(DESTDIR)/$(PREFIX)/bin/contigset2fasta

.PHONY: all clean install
