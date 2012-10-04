ifndef DESTDIR
	export DESTDIR=/opt/dnanexus
endif
ifndef PREFIX
	export PREFIX=/
endif

all:
	mkdir -pv build
	make -C build -f ../Makefile.static all

clean:
	rm -rvf build

dist: all
	make -C build -f ../Makefile.static dist

install: all
	install -v build/ua $(DESTDIR)/$(PREFIX)/bin

.PHONY: all clean dist install
