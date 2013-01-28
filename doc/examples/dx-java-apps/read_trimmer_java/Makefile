CLASSPATH := $(shell echo resources/lib/*.jar | tr ' ' ':'):$(CLASSPATH)
export CLASSPATH

all:
	cp -f $(DNANEXUS_HOME)/lib/java/*.jar resources/opt/dnanexus/lib/java
	javac -d resources src/*.java

clean:
	-rm resources/*.class
	-rm resources/opt/dnanexus/lib/java/*.jar
