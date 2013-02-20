import java.io.*;
import java.util.*;
import org.apache.commons.io.IOUtils;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.dnanexus.*;

public class DXTrimReads {
    public static void main(String[] args) throws Exception {
        System.out.println("This is the DNAnexus Java Read Trimmer Example App");
        ObjectMapper mapper = new ObjectMapper();

        String JobInput = IOUtils.toString(new FileInputStream("job_input.json"));
        JsonNode JobInputJson = (JsonNode)(new MappingJsonFactory().createJsonParser(JobInput).readValueAsTree());

        // TODO: check for presence of params instead of NullPointerException
        String gtableId = JobInputJson.get("reads").get("$dnanexus_link").textValue();
        int trimLength = JobInputJson.get("trimLength").intValue();

        System.out.println("Trimming reads in "+gtableId);

        JsonNode tableDesc = DXAPI.gtableDescribe(gtableId);

        ObjectNode gtableNewInput = DXJSON.getObjectBuilder()
            .put("initializeFrom",
                 DXJSON.getObjectBuilder()
                 .put("project", System.getenv("DX_WORKSPACE_ID"))
                 .put("id", tableDesc.get("id"))
                 .build())
            .build();

        String outputGTableId = DXAPI.gtableNew(gtableNewInput).get("id").textValue();

        int sequenceColumnIndex = -1, qualColumnIndex = -1;
        for (int i=0; i<tableDesc.get("columns").size(); i++) {
            if (tableDesc.get("columns").get(i).get("name").asText().equals("sequence")) {
                sequenceColumnIndex = i;
            } else if (tableDesc.get("columns").get(i).get("name").asText().equals("quality")) {
                qualColumnIndex = i;
            }
        }

        int step = 10000, nextPartIndex = 1;
        for (int i=0; i<tableDesc.get("length").intValue(); i += step) {
            ObjectNode gtableGetInput = DXJSON.getObjectBuilder()
                .put("starting", i)
                .put("limit", step)
                .build();
            ArrayNode outputRows = mapper.createArrayNode();
            for (JsonNode row : DXAPI.gtableGet(gtableId, gtableGetInput).get("data")) {
                ArrayNode arrayRow = (ArrayNode)row;
                // First row is the index - don't send it back. After removing
                // it, the remaining columns should correspond to the GTable
                // schema.
                arrayRow.remove(0);
                String sequence = arrayRow.get(sequenceColumnIndex).textValue();
                String quality = arrayRow.get(qualColumnIndex).textValue();
                String sequenceSubstr = sequence.substring(0, Math.max(sequence.length()-trimLength, 0));
                String qualitySubstr = quality.substring(0, Math.max(quality.length()-trimLength, 0));
                arrayRow.set(sequenceColumnIndex, TextNode.valueOf(sequenceSubstr));
                arrayRow.set(qualColumnIndex, TextNode.valueOf(qualitySubstr));
                outputRows.add(row);
            }
            ObjectNode gtableAddRowsInput = mapper.createObjectNode();
            gtableAddRowsInput.put("part", nextPartIndex++);
            gtableAddRowsInput.put("data", outputRows);
            DXAPI.gtableAddRows(outputGTableId, gtableAddRowsInput);
        }
        DXAPI.gtableClose(outputGTableId);

        ObjectNode jobOutput = DXJSON.getObjectBuilder()
            .put("trimmedReads", makeDXLink(outputGTableId))
            .build();
        mapper.writeValue(new File("job_output.json"), jobOutput);

        System.out.println("Trimming complete!");
    }

    private static ObjectNode makeDXLink(String objectId) {
        return DXJSON.getObjectBuilder().put("$dnanexus_link", objectId).build();
    }

}
