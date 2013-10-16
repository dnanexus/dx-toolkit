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

import org.junit.*;
import com.fasterxml.jackson.databind.*;
import com.dnanexus.DXAPI;
import com.dnanexus.DXEnvironment;

public class DXAPITest {

    @Test public void testDXAPI() throws IOException {
        JsonNode input = (JsonNode)(new MappingJsonFactory().createJsonParser("{}").readValueAsTree());
        JsonNode responseJson = DXAPI.systemFindDataObjects(input);
        org.junit.Assert.assertEquals(responseJson.isObject(), true);
        // System.out.println(responseJson);
    }

    @Test public void testDXAPICustomEnvironment() throws IOException {
        DXEnvironment env = new DXEnvironment.Builder().build();
        JsonNode input = (JsonNode)(new MappingJsonFactory().createJsonParser("{}").readValueAsTree());
        JsonNode responseJson = DXAPI.systemFindDataObjects(input, env);
        org.junit.Assert.assertEquals(responseJson.isObject(), true);

        JsonNode bogusSecCtx = (new MappingJsonFactory().createJsonParser("{\"auth_token_type\":\"Bearer\",\"auth_token\":\"BOGUS\"}").readValueAsTree());
        env = new DXEnvironment.Builder().setSecurityContext(bogusSecCtx).build();
        try {
            DXAPI.systemFindDataObjects(input, env);
        } catch (Exception exn) {
            org.junit.Assert.assertTrue(exn.toString().contains("InvalidAuthentication"));
        }
    }
}
