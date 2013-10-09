# DNAnexus Java API

## Example:

```java
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.dnanexus.DXAPI;

JsonNode input = (JsonNode)(new MappingJsonFactory().createJsonParser("{}").readValueAsTree());
JsonNode root = DXAPI.systemFindDataObjects(input);
System.out.println(root);
```

## Documentation

[Javadocs](http://autodoc.dnanexus.com/bindings/java/current/)

## Development

### Build dependencies

* Maven (`apt-get install maven`)

### Building

From dx-toolkit, run:

    make && make java

### Tests

Ensure you  have logged  in to the  platform with a  valid token  (for example,
using `dx login`). Then, from from dx-toolkit/src/java:

    mvn test
