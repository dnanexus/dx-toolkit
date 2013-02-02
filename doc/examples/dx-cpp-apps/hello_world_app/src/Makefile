all: ../resources/hello_world_app

../resources/hello_world_app: hello_world.cpp
	mkdir -p build && cd build && cmake -D CMAKE_BUILD_TYPE:STRING=release .. && $(MAKE) && cp -f hello_world_app ../../resources

clean:
	rm -rf build/
	rm -f ../resources/hello_world_app

.PHONY: all clean
