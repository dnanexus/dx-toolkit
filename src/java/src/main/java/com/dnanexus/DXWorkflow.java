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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A workflow.
 *
 * <p>
 * Note that these Java bindings do not supply any high-level way to modify the workflow. Please use
 * the command-line tools or see the API documentation for workflows.
 * </p>
 */
public class DXWorkflow extends DXDataObject implements DXExecutable<DXAnalysis> {

    /**
     * Builder class for creating a new {@code DXWorkflow} object. To obtain an instance, call
     * {@link DXWorkflow#newWorkflow()}.
     */
    public static class Builder extends DXDataObject.Builder<Builder, DXWorkflow> {
        private Builder() {
            super();
        }

        private Builder(DXEnvironment env) {
            super(env);
        }

        /**
         * Creates the workflow.
         *
         * @return a {@code DXWorkflow} object corresponding to the newly created object
         */
        @Override
        public DXWorkflow build() {
            JsonNode workflowNewResponseJson = DXAPI.workflowNew(this.buildRequestHash(), this.env);
            return new DXWorkflow(getNewObjectId(workflowNewResponseJson), this.project, this.env);
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
            return MAPPER.valueToTree(new WorkflowNewRequest(this));
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

    }

    /**
     * Contains metadata for a workflow.
     */
    public static class Describe extends DXDataObject.Describe {
        private Describe() {
            super();
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class WorkflowNewRequest extends DataObjectNewRequest {
        public WorkflowNewRequest(Builder builder) {
            super(builder);
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Deserializes a DXWorkflow from JSON containing a DNAnexus link.
     *
     * @param value JSON object map
     *
     * @return data object
     */
    @SuppressWarnings("unused")
    @JsonCreator
    private static DXWorkflow create(Map<String, Object> value) {
        checkDXLinkFormat(value);
        // TODO: how to set the environment?
        return DXWorkflow.getInstance((String) value.get("$dnanexus_link"));
    }

    /**
     * Returns a {@code DXWorkflow} associated with an existing workflow.
     *
     * @throws NullPointerException If {@code workflowId} is null
     */
    public static DXWorkflow getInstance(String workflowId) {
        return new DXWorkflow(workflowId, null);
    }

    /**
     * Returns a {@code DXWorkflow} associated with an existing workflow in a particular project or
     * container.
     *
     * @throws NullPointerException If {@code workflowId} or {@code container} is null
     */
    public static DXWorkflow getInstance(String workflowId, DXContainer project) {
        return new DXWorkflow(workflowId, project, null);
    }

    /**
     * Returns a {@code DXWorkflow} associated with an existing workflow in a particular project
     * using the specified environment.
     *
     * @throws NullPointerException If {@code workflowId} or {@code container} is null
     */
    public static DXWorkflow getInstanceWithEnvironment(String workflowId, DXContainer project,
            DXEnvironment env) {
        Preconditions.checkNotNull(env);
        return new DXWorkflow(workflowId, project, env);
    }

    /**
     * Returns a {@code DXWorkflow} associated with an existing workflow using the specified
     * environment.
     *
     * @throws NullPointerException If {@code workflowId} is null
     */
    public static DXWorkflow getInstanceWithEnvironment(String workflowId, DXEnvironment env) {
        Preconditions.checkNotNull(env);
        return new DXWorkflow(workflowId, env);
    }

    /**
     * Returns a Builder object for creating a new {@code DXWorkflow}.
     *
     * @return a newly initialized builder object
     */
    public static Builder newWorkflow() {
        return new Builder();
    }

    /**
     * Returns a Builder object for creating a new {@code DXWorkflow} using the specified
     * environment.
     *
     * @param env environment to use to make API calls
     *
     * @return a newly initialized builder object
     */
    public static Builder newWorkflowWithEnvironment(DXEnvironment env) {
        return new Builder(env);
    }

    private DXWorkflow(String workflowId, DXContainer project, DXEnvironment env) {
        super(workflowId, project, env);
        // TODO: also verify correct object ID format
        Preconditions.checkArgument(workflowId.startsWith("workflow-"),
                "Workflow ID must start with \"workflow-\"");
    }

    private DXWorkflow(String workflowId, DXEnvironment env) {
        super(workflowId, env);
        Preconditions.checkArgument(workflowId.startsWith("workflow-"),
                "Workflow ID must start with \"workflow-\"");
    }

    @Override
    public DXWorkflow close() {
        super.close();
        return this;
    }

    @Override
    public DXWorkflow closeAndWait() {
        super.closeAndWait();
        return this;
    }

    @Override
    public Describe describe() {
        return DXJSON.safeTreeToValue(apiCallOnObject("describe"), Describe.class);
    }

    @Override
    public ExecutableRunner<DXAnalysis> newRun() {
        return ExecutableRunner.getWorkflowRunnerWithEnvironment(this.getId(), this.env);
    }

}
