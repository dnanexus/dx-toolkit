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
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.Lists;

/**
 * Utility class for working with JSON objects.
 */
public class DXJSON {

    private static final JsonFactory dxJsonFactory = new MappingJsonFactory();
    private static final ObjectMapper mapper = new ObjectMapper();

    // Utility class should not be instantiated
    private DXJSON() {
    }

    /**
     * Parses the specified string into a JSON object.
     */
    public static JsonNode parseJson(String stringified) throws IOException {
        return dxJsonFactory.createParser(stringified).readValueAsTree();
    }

    /**
     * Builder class that generates a JSON array.
     *
     * Example:
     *
     * <pre>
     * {@code
     * ArrayNode a = DXJSON.getArrayBuilder()
     *                     .add("Foo")
     *                     .addAllStrings(ImmutableList.of("Bar", "Baz"))
     *                     .build()}</pre>
     *
     * when serialized, produces the JSON array <tt>["Foo", "Bar", "Baz"]</tt>.
     */
    public static class ArrayBuilder {
        private final boolean isEmpty;
        private final ArrayBuilder next;
        // If non-null, a new node to add to the array
        private final JsonNode value;
        // If non-null, a list of nodes to addAll to the array
        private final List<JsonNode> listValue;

        private ArrayBuilder(boolean isEmpty, ArrayBuilder next, JsonNode value, List<JsonNode> listValue) {
            this.isEmpty = isEmpty;
            this.next = next;
            this.value = value;
            this.listValue = listValue;
        }

        /**
         * Initializes an ArrayBuilder which will generate an empty array.
         */
        public ArrayBuilder() {
            this(true, null, null, null);
        }

        /**
         * Adds the specified JsonNode to the end of the array.
         */
        public ArrayBuilder add(JsonNode value) {
            return new ArrayBuilder(false, this, value, null);
        }

        /**
         * Adds the specified String to the end of the array.
         */
        public ArrayBuilder add(String value) {
            return add(new TextNode(value));
        }

        /**
         * Adds the specified JsonNode objects, in order, to the end of the
         * array.
         */
        public ArrayBuilder addAll(List<JsonNode> values) {
            return new ArrayBuilder(false, this, null, values);
        }

        // Unfortunately addAll(List<String>) has the same erasure as
        // addAll(List<JsonNode>), so only one of these methods can be named
        // addAll.
        /**
         * Adds the specified String objects, in order, to the end of the array.
         */
        public ArrayBuilder addAllStrings(List<String> values) {
            List<JsonNode> jsonNodeValues = Lists.newArrayList();
            for (String value : values) {
                jsonNodeValues.add(new TextNode(value));
            }
            return new ArrayBuilder(false, this, null, jsonNodeValues);
        }

        /**
         * Generates a JSON array.
         */
        public ArrayNode build() {
            ArrayNode output = mapper.createArrayNode();
            ArrayBuilder nextBuilder = this;
            List<ArrayBuilder> builders = Lists.newArrayList();
            // We'll need to process this linked list in the reverse order, so
            // that the first items to be added (which are at the "tail") go
            // into the result list first.
            while (!nextBuilder.isEmpty) {
                builders.add(nextBuilder);
                nextBuilder = nextBuilder.next;
            }
            Collections.reverse(builders);

            for (ArrayBuilder builder : builders) {
                if (builder.value != null) {
                    output.add(builder.value);
                }
                if (builder.listValue != null) {
                    output.addAll(builder.listValue);
                }
            }
            return output;
        }
    }

    /**
     * Builder class that generates a JSON object (hash).
     *
     * Example:
     *
     * <pre>
     * {@code
     * ObjectNode o = DXJSON.getObjectBuilder()
     *                      .put("key1", "a-string")
     *                      .put("key2", 12321)
     *                      .build()}</pre>
     *
     * when serialized, produces the JSON object <tt>{"key1": "a-string", "key2": 12321}</tt>.
     */
    public static class ObjectBuilder {
        private final boolean isEmpty;
        private final ObjectBuilder next;
        private final String key;
        private final JsonNode value;

        private ObjectBuilder(boolean isEmpty, ObjectBuilder next, String key, JsonNode value) {
            this.isEmpty = isEmpty;
            this.next = next;
            this.key = key;
            this.value = value;
        }

        /**
         * Initializes an ObjectBuilder which will generate an empty object.
         */
        public ObjectBuilder() {
            this(true, null, null, null);
        }

        /**
         * Adds a key-value pair with an arbitrary JsonNode value.
         */
        public ObjectBuilder put(String key, JsonNode value) {
            return new ObjectBuilder(false, this, key, value);
        }

        // TODO: allow easy creation of nulls

        /**
         * Adds a key-value pair with a string value and returns the resulting
         * ObjectBuilder.
         */
        public ObjectBuilder put(String key, String value) {
            return put(key, new TextNode(value));
        }

        /**
         * Adds a key-value pair with a numeric value and returns the resulting
         * ObjectBuilder.
         */
        public ObjectBuilder put(String key, int value) {
            return put(key, new IntNode(value));
        }

        /**
         * Adds a key-value pair with a numeric value and returns the resulting
         * ObjectBuilder.
         */
        public ObjectBuilder put(String key, long value) {
            return put(key, new LongNode(value));
        }

        /**
         * Adds a key-value pair with a numeric value and returns the resulting
         * ObjectBuilder.
         */
        public ObjectBuilder put(String key, double value) {
            return put(key, new DoubleNode(value));
        }

        /**
         * Adds a key-value pair with a boolean value and returns the resulting
         * ObjectBuilder.
         */
        public ObjectBuilder put(String key, boolean value) {
            return put(key, value ? BooleanNode.TRUE : BooleanNode.FALSE);
        }

        /**
         * Generates a JSON object.
         */
        public ObjectNode build() {
            ObjectNode output = mapper.createObjectNode();
            ObjectBuilder nextBuilder = this;
            while (!nextBuilder.isEmpty) {
                output.set(nextBuilder.key, nextBuilder.value);
                nextBuilder = nextBuilder.next;
            }
            return output;
        }
    }

    /**
     * Creates a new ArrayBuilder, initialized to produce an empty array.
     */
    public static ArrayBuilder getArrayBuilder() {
        return new ArrayBuilder();
    }

    /**
     * Creates a new ObjectBuilder, initialized to produce an empty object.
     */
    public static ObjectBuilder getObjectBuilder() {
        return new ObjectBuilder();
    }

    /**
     * Translates the given JSON object to an instance of the specified class.
     *
     * <p>
     * This is a wrapper around Jackson's {@code treeToValue} that suppresses
     * {@code JsonProcessingException} and rethrows it as an unchecked exception.
     * </p>
     *
     * @param json JSON object
     * @param valueType A Jackson-deserializable class
     * @return An instance of the class given by {@code klass}
     */
    public static <T> T safeTreeToValue(JsonNode json, Class<T> valueType) {
        try {
            return mapper.treeToValue(json, valueType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
