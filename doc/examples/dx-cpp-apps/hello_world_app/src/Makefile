all: build install

build:
	mkdir -p build && cd build && cmake -D CMAKE_BUILD_TYPE:STRING=RELEASE .. && $(MAKE)

install: build
	cp -f build/hello_world_app ../resources

clean:
	rm -rf build/
	rm -f ../resources/hello_world_app

.PHONY: all clean build install
