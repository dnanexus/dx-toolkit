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

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A record (the minimal data object class).
 */
public class DXRecord extends DXDataObject {

    /**
     * Builder class for creating a new {@code DXRecord} object. To obtain an instance, call
     * {@link DXRecord#newRecord()}.
     */
    public static class Builder extends DXDataObject.Builder<Builder, DXRecord> {

        private Builder() {
            super();
        }

        private Builder(DXEnvironment env) {
            super(env);
        }

        /**
         * Creates the record.
         *
         * @return a {@code DXRecord} object corresponding to the newly created object
         */
        @Override
        public DXRecord build() {
            return new DXRecord(DXAPI.recordNew(this.buildRequestHash(), ObjectNewResponse.class,
                    this.env).getId(), this.project, this.env, null);
        }

        /**
         * Use this method to test the JSON hash created by a particular builder call without
         * actually executing the request.
         *
         * @return a JsonNode
         */
        @VisibleForTesting
        JsonNode buildRequestHash() {
            checkAndFixParameters();
            return mapper.valueToTree(new RecordNewRequest(this));
        }

        /*
         * (non-Javadoc)
         *
         * @see com.dnanexus.DXDataObject.Builder#getThisInstance()
         */
        @Override
        protected Builder getThisInstance() {
            return this;
        }

        // TODO: initializeFrom

    }

    /**
     * Contains metadata for a record.
     */
    public static class Describe extends DXDataObject.Describe {
        // Record implements no fields that are not available on all
        // DXDataObjects, but other classes do.
        private Describe() {
            super();
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class RecordNewRequest extends DataObjectNewRequest {
        // Record implements no fields that are not available on all
        // DXDataObjects, but other classes do.
        public RecordNewRequest(Builder builder) {
            super(builder);
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Deserializes a DXRecord from JSON containing a DNAnexus link.
     *
     * @param value JSON object map
     *
     * @return data object
     */
    @JsonCreator
    private static DXRecord create(Map<String, Object> value) {
        checkDXLinkFormat(value);
        // TODO: how to set the environment?
        return DXRecord.getInstance((String) value.get("$dnanexus_link"));
    }

    /**
     * Returns a {@code DXRecord} associated with an existing record.
     *
     * @throws NullPointerException If {@code recordId} is null
     */
    public static DXRecord getInstance(String recordId) {
        return new DXRecord(recordId, null);
    }

    /**
     * Returns a {@code DXRecord} associated with an existing record in a particular project or
     * container.
     *
     * @throws NullPointerException If {@code recordId} or {@code container} is null
     */
    public static DXRecord getInstance(String recordId, DXContainer project) {
        return new DXRecord(recordId, project, null, null);
    }

    /**
     * Returns a {@code DXRecord} associated with an existing record in a particular project using
     * the specified environment, with the specified cached describe output.
     *
     * <p>
     * This method is for use exclusively by bindings to the "find" routes when describe hashes are
     * returned with the find output.
     * </p>
     *
     * @throws NullPointerException If any argument is null
     */
    static DXRecord getInstanceWithCachedDescribe(String recordId, DXContainer project,
            DXEnvironment env, JsonNode describe) {
        return new DXRecord(recordId, project, Preconditions.checkNotNull(env,
                "env may not be null"), Preconditions.checkNotNull(describe,
                "describe may not be null"));
    }

    /**
     * Returns a {@code DXRecord} associated with an existing record in a particular project using
     * the specified environment.
     */
    public static DXRecord getInstanceWithEnvironment(String recordId, DXContainer project,
            DXEnvironment env) {
        return new DXRecord(recordId, project, Preconditions.checkNotNull(env,
                "env may not be null"), null);
    }

    /**
     * Returns a {@code DXRecord} associated with an existing record using the specified
     * environment.
     *
     * @throws NullPointerException If {@code recordId} is null
     */
    public static DXRecord getInstanceWithEnvironment(String recordId, DXEnvironment env) {
        return new DXRecord(recordId, Preconditions.checkNotNull(env, "env may not be null"));
    }

    /**
     * Returns a Builder object for creating a new {@code DXRecord}.
     *
     * <p>
     * Example use:
     * </p>
     *
     * <pre>
     * DXRecord r = DXRecord.newRecord().inProject(&quot;proj-0000&quot;).setName(&quot;foo&quot;).build();
     * </pre>
     *
     * @return a newly initialized builder object
     */
    public static Builder newRecord() {
        return new Builder();
    }

    /**
     * Returns a Builder object for creating a new {@code DXRecord} using the specified environment.
     *
     * <p>
     * Example use:
     * </p>
     *
     * <pre>
     * DXRecord r = DXRecord.newRecordWithEnvironment(DXEnvironment.create()).inProject(&quot;proj-0000&quot;).setName(&quot;foo&quot;).build();
     * </pre>
     *
     * @param env environment to use to make API calls
     *
     * @return a newly initialized builder object
     */
    public static Builder newRecordWithEnvironment(DXEnvironment env) {
        return new Builder(env);
    }

    private DXRecord(String recordId, DXContainer project, DXEnvironment env, JsonNode describe) {
        super(recordId, "record", project, env, describe);
    }

    private DXRecord(String recordId, DXEnvironment env) {
        super(recordId, "record", env, null);
    }

    @Override
    public DXRecord close() {
        super.close();
        return this;
    }

    @Override
    public DXRecord closeAndWait() {
        super.closeAndWait();
        return this;
    }

    @Override
    public Describe describe() {
        return DXJSON.safeTreeToValue(apiCallOnObject("describe", RetryStrategy.SAFE_TO_RETRY),
                Describe.class);
    }

    @Override
    public Describe describe(DescribeOptions options) {
        return DXJSON.safeTreeToValue(
                apiCallOnObject("describe", MAPPER.valueToTree(options),
                        RetryStrategy.SAFE_TO_RETRY), Describe.class);
    }

    @Override
    public Describe getCachedDescribe() {
        this.checkCachedDescribeAvailable();
        return DXJSON.safeTreeToValue(this.cachedDescribe, Describe.class);
    }

}
