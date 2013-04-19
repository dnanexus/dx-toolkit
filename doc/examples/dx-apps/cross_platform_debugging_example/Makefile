SHELL=/bin/bash -e

all: dispatch_by_os

dispatch_by_os:
	(if [ `python -c 'import platform; print platform.system()'` == "Linux" ]; then \
	  echo "=== Building for Linux... ==="; \
	  $(MAKE) resources/usr/bin/greeter resources/usr/lib/libgreeter.so.1.0.0; \
	fi)
	(if [ `python -c 'import platform; print platform.system()'` == "Darwin" ]; then \
	  echo "=== Building for Mac... ==="; \
	  $(MAKE) mac_resources/usr/bin/greeter mac_resources/usr/lib/libgreeter.1.0.0.dylib; \
	fi)

resources/usr/bin/greeter: src/greeter/main.cpp resources/usr/lib/libgreeter.so.1.0.0
	mkdir -p resources/usr/bin
	g++ src/greeter/main.cpp -o resources/usr/bin/greeter -Lresources/usr/lib -lgreeter

resources/usr/lib/libgreeter.so.1.0.0: src/greeter/greeter.cpp
	mkdir -p resources/usr/lib
	g++ -fPIC -c src/greeter/greeter.cpp -o src/greeter/greeter.o
	g++ -shared -Wl,-soname,libgreeter.so.1.0.0 -o resources/usr/lib/libgreeter.so.1.0.0 src/greeter/greeter.o
	ln -sf libgreeter.so.1.0.0 resources/usr/lib/libgreeter.so

mac_resources/usr/bin/greeter: src/greeter/main.cpp mac_resources/usr/lib/libgreeter.1.0.0.dylib
	mkdir -p mac_resources/usr/bin
	g++ src/greeter/main.cpp -o mac_resources/usr/bin/greeter -Lmac_resources/usr/lib -lgreeter

mac_resources/usr/lib/libgreeter.1.0.0.dylib: src/greeter/greeter.cpp
	mkdir -p mac_resources/usr/lib
	g++ -fPIC -c src/greeter/greeter.cpp -o src/greeter/greeter_mac.o
	g++ -dynamiclib -undefined suppress -flat_namespace src/greeter/greeter_mac.o -o mac_resources/usr/lib/libgreeter.1.0.0.dylib
	ln -sf libgreeter.1.0.0.dylib mac_resources/usr/lib/libgreeter.dylib

.PHONY: all dispatch_by_os
