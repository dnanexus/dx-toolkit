// Copyright (C) 2013-2014 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

package com.dnanexus;

import java.io.IOException;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class DXJobTest {

    @JsonInclude(Include.NON_NULL)
    private static class ExampleDetails {
        @JsonProperty
        public long sampleId;
    }

    @JsonInclude(Include.NON_NULL)
    private static class ExampleInput {
        @JsonProperty
        public String inputParam;
    }

    @JsonInclude(Include.NON_NULL)
    private static class ExampleOutput {
        @JsonProperty
        public String outputParam;
    }

    @Test
    public void testJobDescribeDeserialization() throws IOException {
        JsonNode inputHash = DXJSON.getObjectBuilder().put("inputParam", "input").build();
        JsonNode runInputHash = DXJSON.getObjectBuilder().put("inputParam", "runInput").build();
        JsonNode originalInputHash =
                DXJSON.getObjectBuilder().put("inputParam", "originalInput").build();
        JsonNode outputHash = DXJSON.getObjectBuilder().put("outputParam", "output").build();

        JsonNode describeOutput =
                DXJSON.getObjectBuilder().put("id", "job-000000000000000000000000")
                        .put("analysis", "analysis-010101010101010101010101")
                        .put("applet", "applet-010203040102030401020304")
                        .put("billTo", "user-dnanexus")
                        .put("created", 1234567890000L)
                        .put("delayWorkspaceDestruction", true)
                        .put("details", DXJSON.parseJson("{\"sampleId\": 1234}"))
                        .put("executableName", "my app")
                        .put("folder", "/outputFolder")
                        .put("function", "postprocess")
                        .put("input", inputHash)
                        .put("isFree", false)
                        .put("launchedBy", "user-launcher")
                        .put("modified", 1234567890123L)
                        .put("name", "my job")
                        .put("originJob", "job-010203040102030401020304")
                        .put("originalInput", originalInputHash)
                        .put("output", outputHash)
                        .put("parentAnalysis", "analysis-000011112222333344445555")
                        .put("parentJob", "job-000000000000000000000001")
                        .put("project", "project-234523452345234523452345")
                        .put("projectCache", "container-345634563456345634563456")
                        .put("properties", DXJSON.parseJson("{\"k1\": \"v1\", \"k2\": \"v2\"}"))
                        .put("resources", "container-456745674567456745674567")
                        .put("rootExecution", "analysis-333333333333333333333333")
                        .put("runInput", runInputHash)
                        .put("stage", "stage-123456781234567812345678")
                        .put("startedRunning", 1234567891000L)
                        .put("state", "done")
                        .put("stateTransitions", DXJSON.parseJson("[{\"newState\": \"running\", \"setAt\": 1234567891000}]"))
                        .put("stoppedRunning", 1234567892000L)
                        .put("tags", DXJSON.getArrayBuilder().add("t1").build())
                        .put("totalPrice", 1.0)
                        .put("workspace", "container-343434343434343434343434")
                        .build();

        DXJob.Describe describe =
                new DXJob.Describe(DXJSON.safeTreeToValue(describeOutput,
                        DXJob.DescribeResponseHash.class), DXEnvironment.create());
        DXJob parentJob = DXJob.getInstance("job-000000000000000000000001");

        Assert.assertEquals("job-000000000000000000000000", describe.getId());

        Assert.assertEquals(DXAnalysis.getInstance("analysis-010101010101010101010101"),
                describe.getAnalysis());
        Assert.assertEquals(DXApplet.getInstance("applet-010203040102030401020304"),
                describe.getApplet());
        Assert.assertEquals("user-dnanexus", describe.getBillTo());
        Assert.assertEquals(new Date(1234567890000L), describe.getCreationDate());
        Assert.assertTrue(describe.isWorkspaceDestructionDelayed());
        Assert.assertEquals(1234L, describe.getDetails(ExampleDetails.class).sampleId);
        Assert.assertEquals("my app", describe.getExecutableName());
        Assert.assertEquals("/outputFolder", describe.getFolder());
        Assert.assertEquals("postprocess", describe.getFunction());
        Assert.assertEquals("input", describe.getInput(ExampleInput.class).inputParam);
        Assert.assertFalse(describe.isFree());
        Assert.assertEquals("user-launcher", describe.getLaunchedBy());
        Assert.assertEquals(new Date(1234567890123L), describe.getModifiedDate());
        Assert.assertEquals("my job", describe.getName());
        Assert.assertEquals(DXJob.getInstance("job-010203040102030401020304"),
                describe.getOriginJob());
        Assert.assertEquals("originalInput",
                describe.getOriginalInput(ExampleInput.class).inputParam);
        Assert.assertEquals("output", describe.getOutput(ExampleOutput.class).outputParam);
        Assert.assertEquals(DXAnalysis.getInstance("analysis-000011112222333344445555"),
                describe.getParentAnalysis());
        Assert.assertEquals(parentJob, describe.getParentJob());
        Assert.assertEquals(DXProject.getInstance("project-234523452345234523452345"),
                describe.getProject());
        Assert.assertEquals(DXContainer.getInstance("container-345634563456345634563456"),
                describe.getProjectCache());
        Assert.assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2"), describe.getProperties());
        Assert.assertEquals(DXContainer.getInstance("container-456745674567456745674567"),
                describe.getResources());
        Assert.assertEquals(DXAnalysis.getInstance("analysis-333333333333333333333333"),
                describe.getRootExecution());
        Assert.assertEquals("runInput", describe.getRunInput(ExampleInput.class).inputParam);
        Assert.assertEquals("stage-123456781234567812345678", describe.getStage());
        Assert.assertEquals(new Date(1234567891000L), describe.getStartDate());
        Assert.assertEquals(JobState.DONE, describe.getState());
        Assert.assertEquals(new DXJob.StateTransition(JobState.RUNNING, 1234567891000L),
                Iterables.getOnlyElement(describe.getStateTransitions()));
        Assert.assertEquals(new Date(1234567892000L), describe.getStopDate());
        Assert.assertEquals(ImmutableList.of("t1"), describe.getTags());
        Assert.assertEquals(1.0, describe.getTotalPrice(), 0.0);
        Assert.assertEquals(DXContainer.getInstance("container-343434343434343434343434"),
                describe.getWorkspace());

        // TODO: test failureMessage, failureReason

        // TODO: make test cases with null values and test that they are correctly propagated to the
        // user or throw exceptions as appropriate.

        // Extra fields in the response should not cause us to choke (for API
        // forward compatibility)
        DXJSON.safeTreeToValue(DXJSON.parseJson("{\"notAField\": true}"),
                DXJob.DescribeResponseHash.class);
    }

    @Test
    public void testJobDescribeDeserializationWithNullValues() throws IOException {
        // input, output, runInput and originalInput are missing (as if "io": false were supplied).
        // Ensure that the accessors return IllegalStateException.
        String describeJson = "{\"id\": \"job-000000000000000000000000\"}";

        DXJob.Describe describe = new DXJob.Describe(DXJSON.safeTreeToValue(
                DXJSON.parseJson(describeJson), DXJob.DescribeResponseHash.class),
                DXEnvironment.create());

        Assert.assertEquals("job-000000000000000000000000", describe.getId());
        try {
            describe.getInput(ExampleInput.class);
            Assert.fail("Expected retrieving input to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getOriginalInput(ExampleInput.class);
            Assert.fail("Expected retrieving original input to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getRunInput(ExampleInput.class);
            Assert.fail("Expected retrieving run input to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getOutput(ExampleOutput.class);
            Assert.fail("Expected retrieving output to fail");
        } catch (IllegalStateException e) {
            // Expected
        }

        // output is null (as if the job had not completed yet).
        describeJson = "{\"id\": \"job-000000000000000000000000\", \"output\": null}";

        describe = new DXJob.Describe(DXJSON.safeTreeToValue(DXJSON.parseJson(describeJson),
                DXJob.DescribeResponseHash.class), DXEnvironment.create());

        Assert.assertEquals("job-000000000000000000000000", describe.getId());
        Assert.assertEquals(null, describe.getOutput(ExampleOutput.class));
    }
}
