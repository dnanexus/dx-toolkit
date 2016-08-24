// Copyright (C) 2016 DNAnexus, Inc.
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

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dnanexus.TestEnvironment.ConfigOption;
import com.dnanexus.exceptions.DXAPIException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IdempotencyTest {

    protected static final ObjectMapper mapper = new ObjectMapper();
    protected static final String errorMsg = "InvalidInput: Nonce was reused for an earlier API request that had a different input";

    private DXProject testProject;

    @Before
    public void setUp() {
        testProject = DXProject.newProject().setName("TestProject").build();
    }

    @After
    public void tearDown() {
        if (testProject != null) {
            testProject.destroy(true);
        }
    }

    @Test
    public void testIdempotentAppCreation() {
        if (!TestEnvironment.canRunTest(ConfigOption.ISOLATED_ENV)) {
            System.err.println("Skipping test that would create apps");
            return;
        }

        JsonNode inputSpec = DXJSON.getArrayBuilder()
                .add(DXJSON.getObjectBuilder().put("name", "s").put("class", "string").build()).build();

        ObjectNode input = DXJSON
                  .getObjectBuilder()
                  .put("name", "test_applet")
                  .put("project", testProject.getId())
                  .put("inputSpec", inputSpec)
                  .put("runSpec",
                          DXJSON.getObjectBuilder().put("interpreter", "bash")
                                  .put("code", "#!/bin/bash\n\necho hello world!").build())
                  .put("outputSpec", DXJSON.getArrayBuilder().build()).put("dxapi", "1.0.0").build();

        JsonNode applet = DXAPI.appletNew(input, JsonNode.class);
        String appletId = applet.get("id").asText();

        // Create an app from the previously created applet
        input = DXJSON.getObjectBuilder().put("name", "test_app").put("applet", appletId).put("version", "1")
                .put("nonce", Nonce.nonce()).build();

        JsonNode result1 = DXAPI.appNew(input, JsonNode.class);
        JsonNode result2 = DXAPI.appNew(input, JsonNode.class);
        Assert.assertEquals(result1, result2);

        ObjectNode inputNode = input.deepCopy();
        inputNode.put("name", "diff_name");
        try {
            DXAPI.appNew(inputNode, JsonNode.class);
            Assert.fail("Error, expected request to fail.");
        } catch (DXAPIException e) {
            // Expected exception
            Assert.assertEquals(e.getStatusCode(), 422);
            Assert.assertEquals(errorMsg, e.toString());
        }

        // Test appRun
        JsonNode appRunInput = DXJSON.getObjectBuilder().put("project", testProject.getId())
                .put("input", DXJSON.getObjectBuilder().put("s", "param").build()).put("nonce", Nonce.nonce()).build();

        String appId = result1.get("id").asText();
        result1 = DXAPI.appRun(appId, appRunInput, JsonNode.class);
        result2 = DXAPI.appRun(appId, appRunInput, JsonNode.class);

        Assert.assertEquals(result1, result2);

        inputNode = appRunInput.deepCopy();
        inputNode.put("input", DXJSON.getObjectBuilder().put("s", "different param").build());
        try {
            DXAPI.appRun(appId, inputNode, JsonNode.class);
            Assert.fail("Error, expected request to fail.");
        } catch (DXAPIException e) {
            // Expected exception
            Assert.assertEquals(e.getStatusCode(), 422);
            Assert.assertEquals(errorMsg, e.toString());
        }
    }

    @Test
    public void testIdempotentAppletCreation() {
        ObjectNode input = DXJSON
                  .getObjectBuilder()
                  .put("name", "test_applet")
                  .put("project", testProject.getId())
                  .put("inputSpec", DXJSON.getArrayBuilder().build())
                  .put("runSpec",
                          DXJSON.getObjectBuilder().put("interpreter", "bash")
                                  .put("code", "#!/bin/bash\n\necho hello world!").build()).put("dxapi", "1.0.0")
                  .put("nonce", Nonce.nonce()).build();

        JsonNode result1 = DXAPI.appletNew(input, JsonNode.class);
        JsonNode result2 = DXAPI.appletNew(input, JsonNode.class);
        Assert.assertEquals(result1, result2);

        ObjectNode inputNode = input.deepCopy();
        inputNode.put("name", "diff_name");
        try {
            DXAPI.appletNew(inputNode, JsonNode.class);
            Assert.fail("Error, expected request to fail.");
        } catch (DXAPIException e) {
            // Expected exception
            Assert.assertEquals(e.getStatusCode(), 422);
            Assert.assertEquals(errorMsg, e.toString());
        }
    }

    @Test
    public void testIdempotentAppletRun() {
        JsonNode inputSpec = DXJSON.getArrayBuilder()
                .add(DXJSON.getObjectBuilder().put("name", "s").put("class", "string").build()).build();

        JsonNode input = DXJSON
                .getObjectBuilder()
                .put("name", "test_applet")
                .put("project", testProject.getId())
                .put("inputSpec", inputSpec)
                .put("runSpec",
                        DXJSON.getObjectBuilder().put("interpreter", "bash")
                                .put("code", "#!/bin/bash\n\necho hello world!").build()).put("dxapi", "1.0.0")
                .build();

        JsonNode applet = DXAPI.appletNew(input, JsonNode.class);

        ObjectNode appletRunInput = DXJSON.getObjectBuilder().put("project", testProject.getId())
                  .put("input", DXJSON.getObjectBuilder().put("s", "param").build()).put("nonce", Nonce.nonce()).build();

        String appletId = applet.get("id").asText();

        JsonNode result1 = DXAPI.appletRun(appletId, appletRunInput, JsonNode.class);
        JsonNode result2 = DXAPI.appletRun(appletId, appletRunInput, JsonNode.class);

        Assert.assertEquals(result1, result2);

        ObjectNode inputNode = appletRunInput.deepCopy();
        inputNode.put("input", DXJSON.getObjectBuilder().put("s", "different param").build());
        try {
            DXAPI.appletRun(appletId, inputNode, JsonNode.class);
            Assert.fail("Error, expected request to fail.");
        } catch (DXAPIException e) {
            // Expected exception
            Assert.assertEquals(e.getStatusCode(), 422);
            Assert.assertEquals(errorMsg, e.toString());
        }
    }

    @Test
    public void testIdempotentFileCreation() {
        ObjectNode input = DXJSON.getObjectBuilder().put("name", "test_file").put("project", testProject.getId())
                  .put("nonce", Nonce.nonce()).build();
        JsonNode result1 = DXAPI.fileNew(input, JsonNode.class);
        JsonNode result2 = DXAPI.fileNew(input, JsonNode.class);
        Assert.assertEquals(result1, result2);

        ObjectNode inputNode = input.deepCopy();
        inputNode.put("name", "diff_name");
        try {
            DXAPI.fileNew(inputNode, JsonNode.class);
            Assert.fail("Error, expected request to fail.");
        } catch (DXAPIException e) {
            // Expected exception
            Assert.assertEquals(e.getStatusCode(), 422);
            Assert.assertEquals(errorMsg, e.toString());
        }
    }

    @Test
    public void testIdempotentOrgCreation() {
        if (!TestEnvironment.canRunTest(ConfigOption.ISOLATED_ENV)) {
            System.err.println("Skipping test that would create an org");
            return;
        }

        ObjectNode input = DXJSON.getObjectBuilder().put("name", "test_org").put("handle", "org_handle")
                  .put("nonce", Nonce.nonce()).build();

        JsonNode result1 = DXAPI.orgNew(input, JsonNode.class);
        JsonNode result2 = DXAPI.orgNew(input, JsonNode.class);
        Assert.assertEquals(result1, result2);

        ObjectNode inputNode = input.deepCopy();
        inputNode.put("name", "diff_name");
        try {
            DXAPI.orgNew(inputNode, JsonNode.class);
            Assert.fail("Error, expected request to fail.");
        } catch (DXAPIException e) {
            // Expected exception
            Assert.assertEquals(e.getStatusCode(), 422);
            Assert.assertEquals(errorMsg, e.toString());
        }
    }

    @Test
    public void testIdempotentRecordCreation() {
        ObjectNode input = DXJSON.getObjectBuilder().put("name", "test_record").put("project", testProject.getId())
                .put("nonce", Nonce.nonce()).build();
        JsonNode result1 = DXAPI.recordNew(input, JsonNode.class);
        JsonNode result2 = DXAPI.recordNew(input, JsonNode.class);
        Assert.assertEquals(result1, result2);

        ObjectNode inputNode = input.deepCopy();
        inputNode.put("name", "diff_record_name");
        try {
            DXAPI.recordNew(inputNode, JsonNode.class);
            Assert.fail("Error, expected request to fail.");
        } catch (DXAPIException e) {
            // Expected exception
            Assert.assertEquals(e.getStatusCode(), 422);
            Assert.assertEquals(errorMsg, e.toString());
        }
    }

    @Test
    public void testIdempotentWorkflowCreation() {
        ObjectNode input = DXJSON.getObjectBuilder().put("name", "test_workflow").put("project", testProject.getId())
                .put("nonce", Nonce.nonce()).build();

        JsonNode result1 = DXAPI.workflowNew(input, JsonNode.class);
        JsonNode result2 = DXAPI.workflowNew(input, JsonNode.class);
        Assert.assertEquals(result1, result2);

        ObjectNode inputNode = input.deepCopy();
        inputNode.put("name", "diff_name");
        try {
            DXAPI.workflowNew(inputNode, JsonNode.class);
            Assert.fail("Error, expected request to fail.");
        } catch (DXAPIException e) {
            // Expected exception
            Assert.assertEquals(e.getStatusCode(), 422);
            Assert.assertEquals(errorMsg, e.toString());
        }
    }

    @Test
    public void testInputUpdater() {
        JsonNode inputParams = DXJSON.getObjectBuilder().put("p1", "v1").put("p2", "v2").build();
        JsonNode inputParamsCp = inputParams.deepCopy();
        JsonNode updatedInput = mapper.valueToTree(Nonce.updateNonce(inputParams));
        Assert.assertEquals(inputParams, inputParamsCp);
        Assert.assertTrue(updatedInput.has("nonce"));
        JsonNode updatedInput2 = mapper.valueToTree(Nonce.updateNonce(updatedInput));
        Assert.assertEquals(updatedInput, updatedInput2);
    }

    @Test
    public void testNonceGeneration() {
        Set<String> nonceSet = new HashSet<String>();

        for (int i = 0; i < 1000; i++) {
            String nonce = Nonce.nonce();
            Assert.assertTrue(nonce.length() > 0 && nonce.length() <= 128);

            if (!nonceSet.add(nonce)) {
                System.out.println("Found a duplicate Nonce");
                Assert.fail("Error: Generated a duplicated nonce.");
            }
        }
    }

}
