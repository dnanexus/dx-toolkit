package com.dnanexus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Archival states of file object.
 */
public enum ArchivalState {
    /**
     * The file is in archival storage, such as AWS S3 Glacier or Azure Blob ARCHIVE.
     */
    ARCHIVED("archived"),
    /**
     * The file is in standard storage, such as AWS S3 or Azure Blob.
     */
    LIVE("live"),
    /**
     * Archival requested on the current file, but other copies of the same file are in the live state in multiple
     * projects with the same billTo entity. The file is still in standard storage.
     */
    ARCHIVAL("archival"),
    /**
     * Unarchival requested on the current file. The file is in transition from archival storage to standard storage.
     */
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