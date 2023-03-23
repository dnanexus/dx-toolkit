package com.dnanexus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

public enum UnarchivingRate {
    EXPEDITED("Expedited"),
    STANDARD("Standard"),
    BULK("Bulk");

    private static Map<String, UnarchivingRate> createMap;

    static {
        Map<String, UnarchivingRate> result = Maps.newHashMap();
        for (UnarchivingRate state : UnarchivingRate.values()) {
            result.put(state.getValue(), state);
        }
        createMap = ImmutableMap.copyOf(result);
    }

    @JsonCreator
    private static UnarchivingRate create(String value) {
        return createMap.get(value);
    }

    private String value;

    private UnarchivingRate(String value) {
        this.value = value;
    }

    @JsonValue
    private String getValue() {
        return this.value;
    }
}
