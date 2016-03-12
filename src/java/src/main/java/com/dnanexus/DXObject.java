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

import java.util.regex.Pattern;

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Base class for all objects in the DNAnexus Platform.
 *
 * <p>
 * Two {@link DXObject}s are considered to be equal if they have the same
 * DNAnexus object ID. The environment is not considered when testing for
 * equality.
 * </p>
 */
public abstract class DXObject {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    private static final DXEnvironment DEFAULT_ENVIRONMENT = DXEnvironment.create();

    private static final Pattern CLASS_NAME_RE = Pattern.compile("^[a-z]+$");

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
     * @param dxId DNAnexus object ID, e.g. "file-xxxx"
     * @param className desired class name that should be a prefix of the object ID, or null to
     *        perform no prefix check
     * @param env Environment to use for API queries, or null to use the default environment.
     */
    protected DXObject(String dxId, String className, DXEnvironment env) {
        this.dxId = Preconditions.checkNotNull(dxId, "Object ID may not be null");
        if (className != null) {
            Preconditions.checkArgument(CLASS_NAME_RE.matcher(className).matches(),
                    "className must match [a-z]+");
            String regexp = className + "-" + "[A-Za-z0-9]{24}";
            Preconditions.checkArgument(dxId.matches("^" + regexp + "$"), "dxId must match "
                    + regexp);
        }
        if (env == null) {
            this.env = DEFAULT_ENVIRONMENT;
        } else {
            this.env = env;
        }
    }

    /**
     * Returns the ID of the object.
     *
     * @return the DNAnexus object ID
     */
    public String getId() {
        return this.dxId;
    }

    /**
     * Calls the specified API method on this object (with the specified input hash) and returns its
     * result. Subclasses can use this method to call API methods with the correct object ID and
     * environment settings.
     *
     * @param method Name of method, e.g. "describe"
     * @param input Request payload (to be converted to JSON)
     * @param retryStrategy Indicates whether the request is idempotent and can be retried
     */
    protected JsonNode apiCallOnObject(String method, JsonNode input, RetryStrategy retryStrategy) {
        // TODO: add a higher-level binding for calling common data object
        // methods on data objects of arbitrary class? Here and below
        return new DXHTTPRequest(env).request("/" + this.dxId + "/" + method, input, retryStrategy);
    }

    /**
     * Calls the specified API method on this object (with an empty input hash) and returns its
     * result. Subclasses can use this method to call API methods with the correct object ID and
     * environment settings.
     *
     * @param method Name of method, e.g. "describe"
     * @param retryStrategy Indicates whether the request is idempotent and can be retried
     */
    protected JsonNode apiCallOnObject(String method, RetryStrategy retryStrategy) {
        return apiCallOnObject(method, MAPPER.createObjectNode(), retryStrategy);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof DXObject)) {
            return false;
        }
        DXObject other = (DXObject) obj;
        if (dxId == null) {
            if (other.dxId != null) {
                return false;
            }
        } else if (!dxId.equals(other.dxId)) {
            return false;
        }
        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dxId == null) ? 0 : dxId.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", this.dxId).toString();
    }

}
