all:
	mkdir -pv build
	cd build && cmake ..
	make -C build

clean:
	rm -rvf build

install: all
	cp -v build/ua $(DNANEXUS_HOME)/bin

.PHONY: all
