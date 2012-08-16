all:
	$(MAKE) -C src

install:
	$(MAKE) -C src install

clean:
	rm -rf lib/python
	$(MAKE) -C src clean

.PHONY: all install clean
