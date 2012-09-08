import java.io.*;
import java.util.*;
import org.apache.commons.io.IOUtils;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.dnanexus.*;

public class DXHelloWorld {
    public static void main(String[] args) throws Exception {
        System.out.println("This is the DNAnexus Java Demo App");

        String JobInput = IOUtils.toString(new FileInputStream("job_input.json"));
        JsonNode JobInputJson = (JsonNode)(new MappingJsonFactory().createJsonParser(JobInput).readValueAsTree());
        JsonNode Name = JobInputJson.get("name");

        Map<String,Object> JobOutput = new HashMap<String,Object>();
        JobOutput.put("greeting", "Hello, " + (Name == null ? "World" : Name) + "!");

        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(new File("job_output.json"), JobOutput);
    }
}
