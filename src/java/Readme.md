DXJava: DNAnexus Java API
=========================

`DXJava` is compatible with Java 7 and higher.

[API Documentation](http://autodoc.dnanexus.com/bindings/java/current/)

[Examples of DNAnexus Platform apps written in Java](../../doc/examples/dx-java-apps)

Development
-----------

### Build dependencies

* Maven

```bash
apt-get install maven
````

```bash
brew install maven
```

### Building

From dx-toolkit, run:

    make java

To create a project for Eclipse development:

    cd src/java; mvn eclipse:eclipse

### Tests

Ensure you have logged in to the platform with a valid token (for example,
using `dx login`). Then:

    cd src/java; mvn test

In order to run a particular test class or method, use the `test` option, for example:

    mvn test -Dtest=DXSearchTest

or

    mvn test -Dtest=DXSearchTest#testFindDataObjects

