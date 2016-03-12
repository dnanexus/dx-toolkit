// Copyright (C) 2013-2016 DNAnexus, Inc.
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
