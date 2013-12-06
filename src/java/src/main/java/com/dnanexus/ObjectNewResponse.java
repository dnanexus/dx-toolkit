package com.dnanexus;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Deserialized response from most /class/new routes.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class ObjectNewResponse {

    @JsonProperty
    private String id;

    private ObjectNewResponse() {
        // No-arg constructor for JSON deserialization
    }

    /**
     * Returns the ID of the newly generated object.
     *
     * @return DNAnexus object ID
     */
    public String getId() {
        return id;
    }

}
