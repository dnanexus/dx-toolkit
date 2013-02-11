all: dxcpp dxhttp dxjson tests

dxcpp:
	(cd dxcpp; mkdir -p build; cd build; cmake ..; make dxcpp)

dxhttp:
	(cd SimpleHttpLib; mkdir -p build; cd build; cmake ..; make dxhttp)

dxjson:
	(cd dxjson; mkdir -p build; cd build; cmake ..; make dxjson)

tests:
	$(MAKE) -C test all

clean:
	(cd dxcpp; rm -rf build)
	(cd dxjson; rm -rf build)
	(cd SimpleHttpLib; rm -rf build)
	$(MAKE) -C test clean

.PHONY: dxcpp dxjson dxhttp tests
