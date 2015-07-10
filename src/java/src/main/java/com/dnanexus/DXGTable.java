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

import java.util.List;
import java.util.Map;

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * A GenomicTable (tabular storage supporting queries by genomic coordinates).
 *
 * <p>
 * Note that these Java bindings do not supply any high-level way to upload or download data to or
 * from a GTable. Please use the command-line tools <code>dx import</code> and
 * <code>dx export</code>, or see the <a
 * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables">API documentation for
 * GTables</a>. Also, indices are not supported at this time.
 * </p>
 */
public class DXGTable extends DXDataObject {

    /**
     * Builder class for creating a new {@code DXGTable} object. To obtain an instance, call
     * {@link DXGTable#newGTable(List)}.
     */
    public static class Builder extends DXDataObject.Builder<Builder, DXGTable> {

        List<ColumnSpecification> columns;

        private Builder(List<? extends ColumnSpecification> columns) {
            super();
            this.columns = ImmutableList.copyOf(columns);
        }

        private Builder(List<? extends ColumnSpecification> columns, DXEnvironment env) {
            super(env);
            this.columns = ImmutableList.copyOf(columns);
        }

        /**
         * Creates the GTable.
         *
         * @return a {@code DXGTable} object corresponding to the newly created object
         */
        @Override
        public DXGTable build() {
            return new DXGTable(DXAPI.gtableNew(this.buildRequestHash(), ObjectNewResponse.class,
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
            return MAPPER.valueToTree(new GTableNewRequest(this));
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
     * Contains metadata for a GTable.
     */
    public static class Describe extends DXDataObject.Describe {
        @JsonProperty
        private List<ColumnSpecification> columns;
        @JsonProperty
        private Long size;
        @JsonProperty
        private Long length; // May be null if table is not closing or closed

        private Describe() {
            super();
        }

        /**
         * Returns the number of bytes of storage consumed by the GTable.
         *
         * @return byte size of GTable
         */
        public long getByteSize() {
            Preconditions
                    .checkState(this.size != null,
                            "byte size is not available because it was not retrieved with the describe call");
            return this.size;
        }

        /**
         * Returns the columns of the GTable.
         *
         * @return List of columns in the order they appear in the GTable
         */
        public List<ColumnSpecification> getColumns() {
            Preconditions.checkState(this.columns != null,
                    "columns is not available because it was not retrieved with the describe call");
            return ImmutableList.copyOf(this.columns);
        }

        /**
         * Returns the number of rows in the table.
         *
         * @return Number of rows
         */
        public long getNumRows() {
            Preconditions.checkState(this.length != null,
                    "row count is not available because table was open or "
                            + "because row count was not retrieved with the describe call");
            return this.length;
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class GTableNewRequest extends DataObjectNewRequest {
        @JsonProperty
        private final List<ColumnSpecification> columns;

        public GTableNewRequest(Builder builder) {
            super(builder);
            this.columns = builder.columns;
            // TODO: indices
        }
    }

    /**
     * Deserializes a DXGTable from JSON containing a DNAnexus link.
     *
     * @param value JSON object map
     *
     * @return data object
     */
    @JsonCreator
    private static DXGTable create(Map<String, Object> value) {
        checkDXLinkFormat(value);
        // TODO: how to set the environment?
        return DXGTable.getInstance((String) value.get("$dnanexus_link"));
    }

    /**
     * Returns a {@code DXGTable} associated with an existing GTable.
     *
     * @throws NullPointerException If {@code gtableId} is null
     */
    public static DXGTable getInstance(String gtableId) {
        return new DXGTable(gtableId, null);
    }

    /**
     * Returns a {@code DXGTable} associated with an existing GTable in a particular project or
     * container.
     *
     * @throws NullPointerException If {@code gtableId} or {@code container} is null
     */
    public static DXGTable getInstance(String gtableId, DXContainer project) {
        return new DXGTable(gtableId, project, null, null);
    }

    /**
     * Returns a {@code DXGTable} associated with an existing GTable in a particular project using
     * the specified environment, with the specified cached describe output.
     *
     * <p>
     * This method is for use exclusively by bindings to the "find" routes when describe hashes are
     * returned with the find output.
     * </p>
     *
     * @throws NullPointerException If any argument is null
     */
    static DXGTable getInstanceWithCachedDescribe(String gtableId, DXContainer project,
            DXEnvironment env, JsonNode describe) {
        return new DXGTable(gtableId, project, Preconditions.checkNotNull(env,
                "env may not be null"), Preconditions.checkNotNull(describe,
                "describe may not be null"));
    }

    /**
     * Returns a {@code DXGTable} associated with an existing GTable in a particular project using
     * the specified environment.
     *
     * @throws NullPointerException If {@code gtableId} or {@code container} is null
     */
    public static DXGTable getInstanceWithEnvironment(String gtableId, DXContainer project,
            DXEnvironment env) {
        return new DXGTable(gtableId, project, Preconditions.checkNotNull(env,
                "env may not be null"), null);
    }

    /**
     * Returns a {@code DXGTable} associated with an existing GTable using the specified
     * environment.
     *
     * @throws NullPointerException If {@code gtableId} is null
     */
    public static DXGTable getInstanceWithEnvironment(String gtableId, DXEnvironment env) {
        return new DXGTable(gtableId, Preconditions.checkNotNull(env, "env may not be null"));
    }

    /**
     * Returns a Builder object for creating a new {@code DXGTable} with the specified columns.
     *
     * @param columns column specifications (in the order they will appear in the table)
     *
     * @return a newly initialized builder object
     */
    public static Builder newGTable(List<? extends ColumnSpecification> columns) {
        return new Builder(columns);
    }

    /**
     * Returns a Builder object for creating a new {@code DXGTable} with the specified columns,
     * using the specified environment.
     *
     * @param columns column specifications (in the order they will appear in the table)
     * @param env environment to use to make API calls
     *
     * @return a newly initialized builder object
     */
    public static Builder newGTableWithEnvironment(List<? extends ColumnSpecification> columns,
            DXEnvironment env) {
        return new Builder(columns, env);
    }

    private DXGTable(String gtableId, DXContainer project, DXEnvironment env, JsonNode describe) {
        super(gtableId, "gtable", project, env, describe);
    }

    private DXGTable(String gtableId, DXEnvironment env) {
        super(gtableId, "gtable", env, null);
    }

    @Override
    public DXGTable close() {
        super.close();
        return this;
    }

    @Override
    public DXGTable closeAndWait() {
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

    // TODO: addRows and get

}
