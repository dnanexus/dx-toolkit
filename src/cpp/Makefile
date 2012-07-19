simplehttplib = ./SimpleHttpLib

all: dxcpp dxhttp dxjson dxlog test

dxcpp:
	(cd dxcpp; mkdir -p build; cd build; cmake ..; make dxcpp)

dxhttp:
	(cd SimpleHttpLib; mkdir -p build; cd build; cmake ..; make dxhttp)

dxjson:
	(cd dxjson; mkdir -p build; cd build; cmake ..; make dxjson)

dxlog:
	(cd dxlog; mkdir -p build; cd build; cmake ..; make dxlog)

test:
	(cd test; mkdir -p build; cd build; cmake ..; make)

clean:
	(cd dxcpp; rm -rf build)
	(cd dxjson; rm -rf build)
	(cd SimpleHttpLib; rm -rf build)
	(cd dxlog; rm -rf build)
	(cd test; rm -rf build)

.PHONY: dxcpp dxjson dxhttp dxlog test
