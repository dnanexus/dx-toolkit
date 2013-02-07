all: build install

build:
	mkdir -p build && cd build && cmake -D CMAKE_BUILD_TYPE:STRING=RELEASE .. && $(MAKE)

install: build
	cp -f build/random_gtable_app ../resources

clean:
	rm -rf build/
	rm -f ../resources/random_gtable_app

.PHONY: all clean build install
