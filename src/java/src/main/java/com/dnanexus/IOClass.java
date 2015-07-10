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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * The type of an input or output field of an executable.
 */
public enum IOClass {
    RECORD("record"),
    FILE("file"),
    GTABLE("gtable"),
    APPLET("applet"),
    WORKFLOW("workflow"),

    INT("int"),
    FLOAT("float"),
    STRING("string"),
    BOOLEAN("boolean"),
    HASH("hash"),

    ARRAY_OF_RECORDS("array:record"),
    ARRAY_OF_FILES("array:file"),
    ARRAY_OF_GTABLES("array:gtable"),
    ARRAY_OF_APPLETS("array:applet"),
    ARRAY_OF_WORKFLOWS("array:workflow"),

    ARRAY_OF_INTS("array:int"),
    ARRAY_OF_FLOATS("array:float"),
    ARRAY_OF_STRINGS("array:string"),
    ARRAY_OF_BOOLEANS("array:boolean");

    private static Map<String, IOClass> createMap;

    static {
        Map<String, IOClass> result = Maps.newHashMap();
        for (IOClass state : IOClass.values()) {
            result.put(state.getValue(), state);
        }
        createMap = ImmutableMap.copyOf(result);
    }

    @JsonCreator
    private static IOClass create(String value) {
        return createMap.get(value);
    }

    private String value;

    private IOClass(String value) {
        this.value = value;
    }

    @JsonValue
    private String getValue() {
        return this.value;
    }
}
