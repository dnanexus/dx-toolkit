ifndef DESTDIR
	export DESTDIR=/opt/dnanexus
endif
ifndef PREFIX
	export PREFIX=/
endif
UNAME := $(shell uname)

all:
	mkdir -pv build
ifeq ($(UNAME), MINGW32_NT-6.1)
	make -C build -f ../Makefile.mw all
else
	make -C build -f ../Makefile.static all
endif
clean:
	rm -rvf build

dist: all
ifeq ($(UNAME), MINGW32_NT-6.1)
	make -C build -f ../Makefile.mw dist
else
	make -C build -f ../Makefile.static dist
endif

install: all
	install -v build/ua $(DESTDIR)/$(PREFIX)/bin

.PHONY: all clean dist install
