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

import org.junit.Assert;
import org.junit.Test;

import com.dnanexus.exceptions.InvalidAuthenticationException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DXAPITest {

    @JsonInclude(Include.NON_NULL)
    private static class EmptyFindDataObjectsRequest {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class FindDataObjectsResponse {}

    protected static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Smoke test for calling an API route.
     */
    @Test
    public void testDXAPI() {
        DXAPI.systemFindDataObjects(new EmptyFindDataObjectsRequest(),
                FindDataObjectsResponse.class);
    }

    /**
     * Tests using the new routes with JsonNode objects as input and output.
     *
     * <p>
     * This is the most direct migration path for users of the old-style bindings.
     * </p>
     */
    @Test
    public void testDXAPILegacyCompatibility() {
        JsonNode input = DXJSON.getObjectBuilder().put("name", "DXAPI test project").build();
        JsonNode output = DXAPI.projectNew(input, JsonNode.class);
        String projectId = output.get("id").asText();
        DXAPI.projectDestroy(projectId, JsonNode.class);
    }

    @Test
    public void testDXAPICustomEnvironment() throws IOException {
        DXEnvironment env = DXEnvironment.Builder.fromDefaults().build();
        JsonNode input =
                (JsonNode) (new MappingJsonFactory().createParser("{}").readValueAsTree());
        JsonNode responseJson = DXAPI.systemFindDataObjects(input, JsonNode.class, env);
        Assert.assertEquals(responseJson.isObject(), true);

        DXEnvironment bogusContextEnv =
                DXEnvironment.Builder.fromDefaults().setBearerToken("BOGUS").build();
        try {
            DXAPI.systemFindDataObjects(input, JsonNode.class, bogusContextEnv);
            Assert.fail("Expected request with bogus token to throw InvalidAuthentication");
        } catch (InvalidAuthenticationException e) {
            // Expected
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDXAPIOldStyle() {
        JsonNode input = mapper.createObjectNode();
        JsonNode responseJson = DXAPI.systemFindDataObjects(input);
        Assert.assertEquals(responseJson.isObject(), true);
    }
}
