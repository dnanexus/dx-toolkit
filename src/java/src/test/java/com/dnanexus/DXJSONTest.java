// Copyright (C) 2013-2016 DNAnexus, Inc.
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
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class DXJSONTest {

    // Response classes deliberately without @JsonIgnoreProperties, standing in
    // for a caller's own strict class passed to DXAPI.*(..., outputClass).
    public static class StrictResponse {
        public String id;
        public String name;
        public int count;
    }

    public static class Inner {
        public String id;
    }

    public static class Outer {
        public String id;
        public Inner inner;
        public List<Inner> items;
    }

    public static class RenamedResponse {
        @JsonProperty("file_id")
        public String fileId;
    }

    public static class CreatorResponse {
        private final String id;

        @JsonCreator
        public CreatorResponse(@JsonProperty("id") String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AnnotatedResponse {
        public String id;
    }

    private static JsonNode json(String s) throws IOException {
        return DXJSON.parseJson(s);
    }

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

    @Test
    public void unknownTopLevelFieldsAreIgnored() throws IOException {
        StrictResponse r = DXJSON.safeTreeToValue(
                json("{\"id\":\"file-1\",\"name\":\"a\",\"count\":3,\"newField1\":\"x\",\"newField2\":42}"),
                StrictResponse.class);
        Assert.assertEquals("file-1", r.id);
        Assert.assertEquals("a", r.name);
        Assert.assertEquals(3, r.count);
    }

    @Test
    public void unknownFieldWithComplexValueIsIgnored() throws IOException {
        StrictResponse r = DXJSON.safeTreeToValue(
                json("{\"id\":\"x\",\"extra\":{\"deep\":[1,2,{\"k\":null}],\"flag\":true},\"extraArr\":[[],{}]}"),
                StrictResponse.class);
        Assert.assertEquals("x", r.id);
    }

    @Test
    public void unknownFieldWithNullValueIsIgnored() throws IOException {
        StrictResponse r = DXJSON.safeTreeToValue(json("{\"id\":\"x\",\"extra\":null}"), StrictResponse.class);
        Assert.assertEquals("x", r.id);
    }

    @Test
    public void unknownFieldInNestedObjectIsIgnored() throws IOException {
        Outer o = DXJSON.safeTreeToValue(json("{\"id\":\"o\",\"inner\":{\"id\":\"i\",\"extra\":1}}"), Outer.class);
        Assert.assertEquals("o", o.id);
        Assert.assertEquals("i", o.inner.id);
    }

    @Test
    public void unknownFieldInListElementsIsIgnored() throws IOException {
        Outer o = DXJSON.safeTreeToValue(
                json("{\"items\":[{\"id\":\"a\",\"extra\":1},{\"id\":\"b\",\"other\":[2]}]}"), Outer.class);
        Assert.assertEquals(2, o.items.size());
        Assert.assertEquals("a", o.items.get(0).id);
        Assert.assertEquals("b", o.items.get(1).id);
    }

    @Test
    public void onlyUnknownFieldsYieldsDefaults() throws IOException {
        StrictResponse r = DXJSON.safeTreeToValue(json("{\"newField\":\"x\"}"), StrictResponse.class);
        Assert.assertNull(r.id);
        Assert.assertNull(r.name);
        Assert.assertEquals(0, r.count);
    }

    @Test
    public void jsonCreatorClassIgnoresUnknownFields() throws IOException {
        CreatorResponse r = DXJSON.safeTreeToValue(json("{\"id\":\"c\",\"extra\":\"x\"}"), CreatorResponse.class);
        Assert.assertEquals("c", r.getId());
    }

    // Trade-off of ignoring unknown properties: a key that doesn't match
    // exactly is dropped silently instead of raising an error.
    @Test
    public void caseMismatchedKeyIsSilentlyIgnored() throws IOException {
        StrictResponse r = DXJSON.safeTreeToValue(json("{\"Name\":\"a\"}"), StrictResponse.class);
        Assert.assertNull(r.name);
    }

    @Test
    public void jsonPropertyRenamedFieldIgnoresJavaName() throws IOException {
        RenamedResponse r = DXJSON.safeTreeToValue(json("{\"fileId\":\"f\"}"), RenamedResponse.class);
        Assert.assertNull(r.fileId);
    }

    @Test
    public void emptyObjectYieldsDefaults() throws IOException {
        StrictResponse r = DXJSON.safeTreeToValue(json("{}"), StrictResponse.class);
        Assert.assertNull(r.id);
        Assert.assertEquals(0, r.count);
    }

    @Test
    public void missingKnownFieldIsNull() throws IOException {
        StrictResponse r = DXJSON.safeTreeToValue(json("{\"id\":\"x\"}"), StrictResponse.class);
        Assert.assertEquals("x", r.id);
        Assert.assertNull(r.name);
    }

    @Test
    public void typeMismatchOnKnownFieldStillThrows() throws IOException {
        try {
            DXJSON.safeTreeToValue(json("{\"count\":\"not-a-number\"}"), StrictResponse.class);
            Assert.fail("Expected a RuntimeException for a non-numeric count");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getCause() instanceof InvalidFormatException);
        }
    }

    @Test
    public void wrongRootShapeStillThrows() throws IOException {
        try {
            DXJSON.safeTreeToValue(json("[\"a\",\"b\"]"), StrictResponse.class);
            Assert.fail("Expected a RuntimeException for an array root");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getCause() instanceof MismatchedInputException);
        }
    }

    @Test
    public void annotatedClassStillIgnoresUnknownFields() throws IOException {
        AnnotatedResponse r = DXJSON.safeTreeToValue(json("{\"id\":\"x\",\"extra\":1}"), AnnotatedResponse.class);
        Assert.assertEquals("x", r.id);
    }

    @Test
    public void mapTargetKeepsAllFields() throws IOException {
        Map<?, ?> m = DXJSON.safeTreeToValue(json("{\"id\":\"x\",\"newField\":1}"), Map.class);
        Assert.assertEquals(2, m.size());
        Assert.assertEquals("x", m.get("id"));
        Assert.assertEquals(1, m.get("newField"));
    }

    @Test
    public void jsonNodeTargetKeepsAllFields() throws IOException {
        JsonNode n = DXJSON.safeTreeToValue(json("{\"id\":\"x\",\"newField\":1}"), JsonNode.class);
        Assert.assertTrue(n.has("newField"));
    }

}
