package com.dnanexus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

public enum ArchivalState {
    ARCHIVED("archived"),
    LIVE("live"),
    ARCHIVAL("archival"),
    UNARCHIVING("unarchiving");

    private static Map<String, ArchivalState> createMap;

    static {
        Map<String, ArchivalState> result = Maps.newHashMap();
        for (ArchivalState state : ArchivalState.values()) {
            result.put(state.getValue(), state);
        }
        createMap = ImmutableMap.copyOf(result);
    }

    @JsonCreator
    private static ArchivalState create(String value) {
        return createMap.get(value);
    }

    private String value;

    private ArchivalState(String value) {
        this.value = value;
    }

    @JsonValue
    private String getValue() {
        return this.value;
    }
}