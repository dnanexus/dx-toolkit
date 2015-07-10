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
 * A permission level that a user can be given in a particular project or container.
 */
public enum AccessLevel {
    /**
     * User has no permissions.
     */
    NONE(null),
    /**
     * Allows read-only access to data objects and their metadata.
     */
    VIEW("VIEW"),
    /**
     * Allows the user to create new folders and data objects, modify the metadata of open objects,
     * modify open GenomicTables and files, close data objects, and everything allowed by VIEW.
     */
    UPLOAD("UPLOAD"),
    /**
     * Allows the user to modify the contents of all types of data objects, delete objects (if the
     * "PROTECTED" flag on the container is set to false), and everything allowed by UPLOAD.
     */
    CONTRIBUTE("CONTRIBUTE"),
    /**
     * Allows the user to modify the member list and to modify or delete the container, and
     * everything allowed by CONTRIBUTE.
     */
    ADMINISTER("ADMINISTER");

    private static Map<String, AccessLevel> createMap;

    static {
        Map<String, AccessLevel> result = Maps.newHashMap();
        for (AccessLevel state : AccessLevel.values()) {
            if (state.getValue() != null) {
                result.put(state.getValue(), state);
            }
        }
        createMap = ImmutableMap.copyOf(result);
    }

    @JsonCreator
    private static AccessLevel create(String value) {
        if (value == null) {
            return AccessLevel.NONE;
        }
        return createMap.get(value);
    }

    private String value;

    private AccessLevel(String value) {
        this.value = value;
    }

    @JsonValue
    private String getValue() {
        return this.value;
    }
}
