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

import org.junit.*;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.dnanexus.DXJSON;

public class DXJSONTest {
    @BeforeClass public static void setUpClass() throws Exception {
        // Code executed before the first test method
    }

    @Before public void setUp() throws Exception {
        // Code executed before each test
    }

    @Test public void testDXJSON() throws Exception {
        ObjectNode actual1 = DXJSON.getObjectBuilder().build();
        ObjectNode expected1 = new MappingJsonFactory().createJsonParser("{}").readValueAsTree();

        org.junit.Assert.assertEquals(expected1, actual1);
        org.junit.Assert.assertEquals(expected1.toString(), "{}");

        ObjectNode actual2 = DXJSON.getObjectBuilder()
            .put("key1", "a-string")
            .put("key2", 12321)
            .build();
        ObjectNode expected2 = new MappingJsonFactory()
            .createJsonParser("{\"key1\": \"a-string\", \"key2\": 12321}")
            .readValueAsTree();

        org.junit.Assert.assertEquals(expected2, actual2);
    }

    @After public void tearDown() throws Exception {
        // Code executed after each test
    }

    @AfterClass public static void tearDownClass() throws Exception {
        // Code executed after the last test method
    }
}
