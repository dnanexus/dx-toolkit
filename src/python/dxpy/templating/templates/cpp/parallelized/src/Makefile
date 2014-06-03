all: build install

build:
	mkdir -p build && cd build && cmake -D CMAKE_BUILD_TYPE:STRING=RELEASE .. && $(MAKE)

install: build
	mkdir -p ../resources/usr/bin
	cp -f build/DX_APP_WIZARD_NAME ../resources/usr/bin

clean:
	rm -rf build/
	rm -f ../resources/usr/bin/DX_APP_WIZARD_NAME

.PHONY: all clean build install
