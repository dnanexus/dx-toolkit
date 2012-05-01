simplehttplib = ./SimpleHttpLib

all:
	(cd dxcpp; mkdir -p build; cd build; cmake ..; make)
	(cd SimpleHttpLib; mkdir -p build; cd build; cmake ..; make)

clean:
	(cd dxcpp; rm -rf build)
	(cd SimpleHttpLib; rm -rf build)
