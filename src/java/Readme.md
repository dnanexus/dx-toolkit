# DNAnexus Java API

Example:
```java
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.dnanexus.DXAPI;

JsonNode input = (JsonNode)(new MappingJsonFactory().createJsonParser("{}").readValueAsTree());
JsonNode root = DXAPI.systemFindDataObjects(input);
System.out.println(root);
```

## TODO
* Javadoc
* Better JSON handling examples
* dxpy.__init__ feature parity and consistency
