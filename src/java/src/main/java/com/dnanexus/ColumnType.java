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
 * The type of a column in a GTable.
 */
public enum ColumnType {
    /**
     * True or false
     */
    BOOLEAN("boolean"),
    /**
     * Integer in the range 0 to 255
     */
    UINT8("uint8"),
    /**
     * Integer in the range -32,768 to 32,767
     */
    INT16("int16"),
    /**
     * Integer in the range 0 to 65,636
     */
    UINT16("uint16"),
    /**
     * Integer in the range -2,147,483,648 to 2,147,483,647
     */
    INT32("int32"),
    /**
     * Integer in the range 0 to 4,294,967,295
     */
    UINT32("uint32"),
    /**
     * An integer between -2<sup>63</sup> and 2<sup>63</sup>-1 that can be represented by an IEEE
     * 754 double-precision number. This includes but is not limited to all integers between
     * -9,007,199,254,740,992 and 9,007,199,254,740,992.
     *
     * <p>
     * WARNING: this type does not have the full range of a signed 64-bit integer.
     * </p>
     */
    INT64("int64"),
    /**
     * Single-precision floating-point number as defined in IEEE 754
     */
    FLOAT("float"),
    /**
     * Double-precision floating-point number as defined in IEEE 754
     */
    DOUBLE("double"),
    /**
     * Unicode string of variable length
     */
    STRING("string");

    private static Map<String, ColumnType> createMap;

    static {
        Map<String, ColumnType> result = Maps.newHashMap();
        for (ColumnType state : ColumnType.values()) {
            result.put(state.getValue(), state);
        }
        createMap = ImmutableMap.copyOf(result);
    }

    @JsonCreator
    private static ColumnType create(String value) {
        return createMap.get(value);
    }

    private String value;

    private ColumnType(String value) {
        this.value = value;
    }

    @JsonValue
    private String getValue() {
        return this.value;
    }
}
