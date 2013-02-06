all: ../resources/random_gtable_app

../resources/random_gtable_app: rand_gtable.cpp
	mkdir -p build && cd build && cmake -D CMAKE_BUILD_TYPE:STRING=release .. && $(MAKE) && cp -f random_gtable_app ../../resources

clean:
	rm -rf build/
	rm -f ../resources/random_gtable_app

.PHONY: all clean
