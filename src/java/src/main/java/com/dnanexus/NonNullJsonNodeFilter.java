package com.dnanexus;

import com.fasterxml.jackson.databind.JsonNode;

// This excludes matching value, where JsonNode.isNull()
public class NonNullJsonNodeFilter {
    @Override
    public boolean equals(Object o) {
        JsonNode o1 = (JsonNode) o;
        return o1.isNull();
    }
}
