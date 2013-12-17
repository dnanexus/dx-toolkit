import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.dnanexus.ColumnSpecification;
import com.dnanexus.DXAPI;
import com.dnanexus.DXEnvironment;
import com.dnanexus.DXGTable;
import com.dnanexus.DXJSON;
import com.dnanexus.DXUtil;

public class DXTrimReads {

    private static class GTableAddRowsRequest {
        @JsonProperty
        private int part;
        @JsonProperty
        private List<ArrayNode> data;

        public GTableAddRowsRequest(int part, List<ArrayNode> data) {
            this.part = part;
            this.data = data;
        }
    }

    private static class GTableGetRequest {
        @JsonProperty
        private int starting;
        @JsonProperty
        private int limit;

        public GTableGetRequest(int starting, int limit) {
            this.starting = starting;
            this.limit = limit;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class GTableGetResponse {
        @JsonProperty
        private List<ArrayNode> data;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class GTableNewResponse {
        @JsonProperty
        public String id;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ReadTrimmerInput {
        @JsonProperty
        private DXGTable reads;
        @JsonProperty
        private int trimLength;
    }

    private static class ReadTrimmerOutput {
        @JsonProperty
        private DXGTable trimmedReads;

        public ReadTrimmerOutput(DXGTable trimmedReads) {
            this.trimmedReads = trimmedReads;
        }
    }


    public static void main(String[] args) throws IOException {
        System.out.println("This is the DNAnexus Java Read Trimmer Example App");

        ReadTrimmerInput input = DXUtil.getJobInput(ReadTrimmerInput.class);
        DXGTable readsTable = input.reads;
        int trimLength = input.trimLength;

        System.out.println("Trimming reads in " + readsTable.getId());

        // Raw API call here because DXGTable.Builder doesn't support
        // initializeFrom
        ObjectNode gtableNewInput = DXJSON.getObjectBuilder()
            .put("initializeFrom",
                 DXJSON.getObjectBuilder()
                 .put("project", DXEnvironment.create().getWorkspace().getId())
                 .put("id", readsTable.getId())
                 .build())
            .build();
        DXGTable trimmedReads = DXGTable.getInstance(DXAPI.gtableNew(gtableNewInput, GTableNewResponse.class).id);

        DXGTable.Describe gtableDesc = readsTable.describe();

        int sequenceColumnIndex = -1, qualColumnIndex = -1;
        List<ColumnSpecification> columns = gtableDesc.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnSpecification cs = columns.get(i);
            if (cs.getName().equals("sequence")) {
                sequenceColumnIndex = i;
            } else if (cs.getName().equals("quality")) {
                qualColumnIndex = i;
            }
        }

        // Raw API calls here because there are no high-level bindings to "get"
        // and "addRows" methods in DXGTable
        int step = 10000, nextPartIndex = 1;
        for (int i = 0; i < gtableDesc.getNumRows(); i += step) {
            GTableGetRequest gtableGetInput = new GTableGetRequest(i, step);
            List<ArrayNode> inputRows = DXAPI.gtableGet(readsTable.getId(), gtableGetInput, GTableGetResponse.class).data;
            List<ArrayNode> outputRows = new ArrayList<ArrayNode>();
            for (ArrayNode row : inputRows) {
                // First row is the index - don't send it back. After removing
                // it, the remaining columns should correspond to the GTable
                // schema.
                row.remove(0);
                String sequence = row.get(sequenceColumnIndex).textValue();
                String quality = row.get(qualColumnIndex).textValue();
                String sequenceSubstr = sequence.substring(0, Math.max(sequence.length()-trimLength, 0));
                String qualitySubstr = quality.substring(0, Math.max(quality.length()-trimLength, 0));
                row.set(sequenceColumnIndex, TextNode.valueOf(sequenceSubstr));
                row.set(qualColumnIndex, TextNode.valueOf(qualitySubstr));
                outputRows.add(row);
            }
            DXAPI.gtableAddRows(trimmedReads.getId(), new GTableAddRowsRequest(nextPartIndex++, outputRows), JsonNode.class);
        }

        trimmedReads.close();

        DXUtil.writeJobOutput(new ReadTrimmerOutput(trimmedReads));

        System.out.println("Trimming complete!");
    }

}
