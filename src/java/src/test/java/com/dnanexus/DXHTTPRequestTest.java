// Copyright (C) 2013 DNAnexus, Inc.
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
import com.fasterxml.jackson.databind.*;
import com.dnanexus.DXHTTPRequest;
import com.dnanexus.DXEnvironment;
import com.dnanexus.exceptions.InvalidAuthenticationException;
import com.dnanexus.exceptions.InvalidInputException;

public class DXHTTPRequestTest {

    @Test
    public void testDXAPI() throws IOException {
        DXHTTPRequest c = new DXHTTPRequest();
        JsonNode responseJson = c.request("/system/findDataObjects", DXJSON.parseJson("{}"));
        Assert.assertEquals(responseJson.isObject(), true);

        // System.out.println(responseJson);

        String responseText = c.request("/system/findDataObjects", "{}");
        Assert.assertEquals(responseText.substring(0, 1), "{");

        // Tests deserialization of InvalidInput
        DXHTTPRequest c2 = new DXHTTPRequest();
        try {
            c2.request("/system/findDataObjects", DXJSON.parseJson("{\"state\": {\"invalid\": \"oops\"}}"));
            Assert.fail("Expected findDataObjects to fail with InvalidInput");
        } catch (InvalidInputException e) {
            // Error message should be something like
            // "expected key \"state\" of input to be a string"
            Assert.assertTrue(e.toString().contains("key \"state\""));
            Assert.assertEquals(422, e.getStatusCode());
        }
    }

    @Test
    public void testDXAPICustomEnvironment() throws IOException {
        DXEnvironment env = DXEnvironment.create();
        DXHTTPRequest c = new DXHTTPRequest(env);
        JsonNode responseJson = c.request("/system/findDataObjects", DXJSON.parseJson("{}"));
        Assert.assertEquals(responseJson.isObject(), true);

        // System.out.println(responseJson);

        String responseText = c.request("/system/findDataObjects", "{}");
        Assert.assertEquals(responseText.substring(0, 1), "{");

        // Tests deserialization of InvalidAuthentication
        JsonNode bogusSecCtx = DXJSON.parseJson("{\"auth_token_type\":\"Bearer\",\"auth_token\":\"BOGUS\"}");
        env = new DXEnvironment.Builder().setSecurityContext(bogusSecCtx).build();
        DXHTTPRequest c2 = new DXHTTPRequest(env);
        try {
            c2.request("/system/findDataObjects", DXJSON.parseJson("{}"));
            Assert.fail("Expected findDataObjects to fail with InvalidAuthentication");
        } catch (InvalidAuthenticationException e) {
            // Error message should be something like
            // "the token could not be found"
            Assert.assertTrue(e.toString().contains("token"));
            Assert.assertEquals(401, e.getStatusCode());
        }
    }

}
