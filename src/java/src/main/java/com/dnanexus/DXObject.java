// Copyright (C) 2013 DNAnexus, Inc.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

/**
 * Base class for all objects in the DNAnexus Platform.
 */
public abstract class DXObject {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    private static final DXEnvironment DEFAULT_ENVIRONMENT = DXEnvironment.create();

    /**
     * The ID of this object (this is the base for URLs used in API calls,
     * {@code /object-id/verb}).
     */
    protected final String dxId;
    /**
     * Environment object to use for API calls. This should also be propagated
     * to any new object handlers created by this object.
     */
    protected final DXEnvironment env;

    /**
     * Creates a new object with the specified ID and environment.
     *
     * @param dxId
     *            DNAnexus object ID, e.g. "file-xxxx"
     * @param env
     *            Environment to use for API queries, or null to use the default
     *            environment.
     */
    protected DXObject(String dxId, DXEnvironment env) {
        Preconditions.checkNotNull(dxId);
        this.dxId = dxId;
        if (env == null) {
            this.env = DEFAULT_ENVIRONMENT;
        } else {
            this.env = env;
        }
    }

    /**
     * Returns the ID of the object.
     */
    public String getId() {
        return this.dxId;
    }

    /**
     * Calls the specified API method on this object (with the specified input
     * hash) and returns its result. Subclasses can use this method to call API
     * methods with the correct object ID and environment settings.
     */
    protected JsonNode apiCallOnObject(String method, JsonNode input) {
        // TODO: add a higher-level binding for calling common data object
        // methods on data objects of arbitrary class? Here and below
        return new DXHTTPRequest(env).request("/" + this.dxId + "/" + method, input);
    }

    /**
     * Calls the specified API method on this object and returns its result.
     * Subclasses can use this method to call API methods with the correct
     * object ID and environment settings.
     */
    protected JsonNode apiCallOnObject(String method) {
        return apiCallOnObject(method, MAPPER.createObjectNode());
    }
}
