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
 * The state of a data object. Typically "open" objects are write-only and "closed" objects are
 * read-only.
 *
 * <p>
 * For more information, see the API documentation for the <a
 * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Data-Object-Lifecycle">data object
 * lifecycle</a>.
 * </p>
 */
public enum DataObjectState {
    OPEN("open"),
    CLOSING("closing"),
    CLOSED("closed"),
    ABANDONED("abandoned");

    private static Map<String, DataObjectState> createMap;

    static {
        Map<String, DataObjectState> result = Maps.newHashMap();
        for (DataObjectState state : DataObjectState.values()) {
            result.put(state.getValue(), state);
        }
        createMap = ImmutableMap.copyOf(result);
    }

    @JsonCreator
    private static DataObjectState create(String value) {
        return createMap.get(value);
    }

    private String value;

    private DataObjectState(String value) {
        this.value = value;
    }

    @JsonValue
    private String getValue() {
        return this.value;
    }
}
