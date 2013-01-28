import java.io.*;
import org.apache.commons.io.IOUtils;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.dnanexus.*;

public class DXHelloWorld {
    public static void main(String[] args) throws Exception {
        System.out.println("This is the DNAnexus Java Demo App");

        String JobInput = IOUtils.toString(new FileInputStream("job_input.json"));
        JsonNode JobInputJson = new MappingJsonFactory().createJsonParser(JobInput).readValueAsTree();
        JsonNode Name = JobInputJson.get("name");

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode JobOutput = mapper.createObjectNode();
        JobOutput.put("greeting", "Hello, " + (Name == null ? "World" : Name) + "!");
        mapper.writeValue(new File("job_output.json"), JobOutput);
    }
}
