import java.io.*;
import java.util.*;
import org.apache.commons.io.IOUtils;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.dnanexus.*;

/**
 * This class implements the main entry point (only) of the parallelism_java
 * example app. See the Readme.md file (one directory up) for more info about
 * what this app does and how all the pieces fit together.
 */
public class DXParallelismExample {

    private static ObjectNode makeDXLink(String objectId) {
        return DXJSON.getObjectBuilder().put("$dnanexus_link", objectId).build();
    }

    private static ObjectNode makeJbor(String jobId, String fieldName) {
        return DXJSON.getObjectBuilder().put("job", jobId).put("field", fieldName).build();
    }

    public static void main(String[] args) throws IOException {
        System.out.println("This is the DNAnexus Java Parallelism Example App.");
        ObjectMapper mapper = new ObjectMapper();

        String jobInput = IOUtils.toString(new FileInputStream("job_input.json"));
        JsonNode jobInputJson = (JsonNode)(new MappingJsonFactory().createJsonParser(jobInput).readValueAsTree());

        String inputFile = jobInputJson.get("input_file").get("$dnanexus_link").textValue();
        int numSubtasks = jobInputJson.get("num_subtasks").intValue();

        System.out.println("Spawning " + Integer.toString(numSubtasks) + " subtasks");

        List<String> processJobIds = new ArrayList<String>();

        // Spawn (numSubtasks) subjobs.
        //
        // In this example we just pass the input file through to all subtasks.
        for (int i = 0; i < numSubtasks; i++) {
            // Run process job via /job/new:
            //
            // {
            //   'function': 'process',
            //   input: {
            //     index: i,
            //     input_file: {$dnanexus_link: "file-XXXX"}
            //   }
            // }
            // => {id: 'job-XXXX'}
            ObjectNode processJobInputHash = DXJSON.getObjectBuilder()
                .put("function", "process")
                .put("input", DXJSON.getObjectBuilder()
                                    .put("index", i)
                                    .put("input_file", makeDXLink(inputFile))
                                    .build())
                .build();
            String processJobId = DXAPI.jobNew(processJobInputHash).get("id").textValue();
            processJobIds.add(processJobId);
        }

        // Convert bare job ID to a job-based object reference.
        ArrayNode processOutputObjects = mapper.createArrayNode();
        for (String processJobId : processJobIds) {
            processOutputObjects.add(makeJbor(processJobId, "output_file"));
        }

        // The postprocess job takes as input all the outputs from the process
        // jobs.
        //
        // Run postprocess job via /job/new:
        //
        // {
        //   'function': 'postprocess',
        //   input: {
        //     process_outputs: [{job: 'job-XXXX', field: 'output'}, ...]
        //   }
        // }
        // => {id: 'job-XXXX'}
        ObjectNode postprocessJobInputHash = DXJSON.getObjectBuilder()
            .put("function", "postprocess")
            .put("input", DXJSON.getObjectBuilder()
                 .put("process_outputs", processOutputObjects)
                 .build())
            .build();
        String postprocessJobId = DXAPI.jobNew(postprocessJobInputHash).get("id").textValue();

        // Wire the output of the postprocess job up to the output of the app.
        ObjectNode jobOutput = DXJSON.getObjectBuilder()
            .put("output_file", makeJbor(postprocessJobId, "combined_output"))
            .build();
        mapper.writeValue(new File("job_output.json"), jobOutput);
    }

}
