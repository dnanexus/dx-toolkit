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
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class DXJSONTest {

    @Test
    public void testJsonObjects() throws IOException {
        ObjectNode actual1 = DXJSON.getObjectBuilder().build();
        ObjectNode expected1 = new MappingJsonFactory().createParser("{}").readValueAsTree();

        Assert.assertEquals(expected1, actual1);
        Assert.assertEquals(expected1.toString(), "{}");

        ObjectNode actual2 = DXJSON.getObjectBuilder().put("key1", "a-string").put("key2", 12321).build();
        ObjectNode expected2 = new MappingJsonFactory().createParser("{\"key1\": \"a-string\", \"key2\": 12321}")
                .readValueAsTree();

        Assert.assertEquals(expected2, actual2);
    }

    @Test
    public void testJsonArrays() {
        ArrayNode actual1 = DXJSON.getArrayBuilder().add("Foo").addAllStrings(ImmutableList.of("Bar", "Baz")).build();
        List<JsonNode> jsonNodeList1 = Lists.newArrayList(actual1);
        Assert.assertEquals(3, jsonNodeList1.size());
        Assert.assertEquals("\"Foo\"", jsonNodeList1.get(0).toString());
        Assert.assertEquals("\"Bar\"", jsonNodeList1.get(1).toString());
        Assert.assertEquals("\"Baz\"", jsonNodeList1.get(2).toString());

        ArrayNode actual2 = DXJSON.getArrayBuilder().build();
        List<JsonNode> jsonNodeList2 = Lists.newArrayList(actual2);
        Assert.assertEquals(0, jsonNodeList2.size());
    }

}
