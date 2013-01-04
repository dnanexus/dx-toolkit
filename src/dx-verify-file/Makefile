ifndef DESTDIR
	export DESTDIR=/opt/dnanexus
endif
ifndef PREFIX
	export PREFIX=/
endif
UNAME := $(shell uname)

all:
	mkdir -pv build
	cd build && cmake ..
	$(MAKE) -C build

clean:
	rm -rvf build build_static

install: all
	install -v build/dx-verify-file $(DESTDIR)/$(PREFIX)/bin

build_static:
	mkdir -pv build_static
ifeq ($(UNAME), MINGW32_NT-6.1)
	make -C build_static -f ../Makefile.mw all
else
	make -C build_static -f ../Makefile.static all
endif

dist_static: build_static
ifeq ($(UNAME), MINGW32_NT-6.1)
	make -C build_static -f ../Makefile.mw dist
else
	make -C build_static -f ../Makefile.static dist
endif

.PHONY: all clean install static_build static_dist

#ifndef DESTDIR
#	export DESTDIR=/opt/dnanexus
#endif
#ifndef PREFIX
#	export PREFIX=/
#endif
#UNAME := $(shell uname)
#
#all:
#	mkdir -pv build
#ifeq ($(UNAME), MINGW32_NT-6.1)
#	make -C build -f ../Makefile.mw all
#else
#	make -C build -f ../Makefile.static all
#endif
#clean:
#	rm -rvf build
#
#dist: all
#ifeq ($(UNAME), MINGW32_NT-6.1)
#	make -C build -f ../Makefile.mw dist
#else
#	make -C build -f ../Makefile.static dist
#endif
#
#install: all
#	install -v build/ua $(DESTDIR)/$(PREFIX)/bin
#
#.PHONY: all clean dist install
