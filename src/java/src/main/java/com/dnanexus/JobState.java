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
 * The state of a job in the system.
 */
public enum JobState {
    IDLE("idle"),
    WAITING_ON_INPUT("waiting_on_input"),
    RUNNABLE("runnable"),
    RUNNING("running"),
    WAITING_ON_OUTPUT("waiting_on_output"),
    DONE("done"),
    FAILED("failed"),
    UNRESPONSIVE("unresponsive"),
    TERMINATING("terminating"),
    TERMINATED("terminated");

    private static Map<String, JobState> createMap;

    static {
        Map<String, JobState> result = Maps.newHashMap();
        for (JobState state : JobState.values()) {
            result.put(state.getValue(), state);
        }
        createMap = ImmutableMap.copyOf(result);
    }

    @JsonCreator
    private static JobState create(String value) {
        return createMap.get(value);
    }

    private String value;

    private JobState(String value) {
        this.value = value;
    }

    @JsonValue
    private String getValue() {
        return this.value;
    }

}
