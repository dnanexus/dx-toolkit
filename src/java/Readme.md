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
# if you don't have openjdk installed maven installation will install the latest version
brew install maven

# if you already have one jdk and don't want to have multiple versions
brew install --ignore-dependencies maven

# if Java was installed with brew check that symlink is correct, e.g. for openjdk@11
sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
# verify with
java -version
# Make sure you have JAVA_HOME set for Maven
export JAVA_HOME=$(/usr/libexec/java_home -V)
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

