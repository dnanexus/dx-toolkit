all: build install

build:
	mkdir -p build && cd build && cmake -D CMAKE_BUILD_TYPE:STRING=RELEASE .. && $(MAKE)

install: build
	cp -f build/DX_APP_WIZARD_NAME ../resources

clean:
	rm -rf build/
	rm -f ../resources/DX_APP_WIZARD_NAME

.PHONY: all clean build install
