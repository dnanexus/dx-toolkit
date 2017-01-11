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

import java.util.Map;

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
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
            return new DXWorkflow(DXAPI.workflowNew(this.buildRequestHash(),
                    ObjectNewResponse.class, this.env).getId(), this.project, this.env, null);
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

    /**
     * Deserializes a DXWorkflow from JSON containing a DNAnexus link.
     *
     * @param value JSON object map
     *
     * @return data object
     */
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
        return new DXWorkflow(workflowId, project, null, null);
    }

    /**
     * Returns a {@code DXWorkflow} associated with an existing workflow in a particular project
     * using the specified environment, with the specified cached describe output.
     *
     * <p>
     * This method is for use exclusively by bindings to the "find" routes when describe hashes are
     * returned with the find output.
     * </p>
     *
     * @throws NullPointerException If any argument is null
     */
    static DXWorkflow getInstanceWithCachedDescribe(String workflowId, DXContainer project,
            DXEnvironment env, JsonNode describe) {
        return new DXWorkflow(workflowId, project, Preconditions.checkNotNull(env,
                "env may not be null"), Preconditions.checkNotNull(describe,
                "describe may not be null"));
    }

    /**
     * Returns a {@code DXWorkflow} associated with an existing workflow in a particular project
     * using the specified environment.
     *
     * @throws NullPointerException If {@code workflowId} or {@code container} is null
     */
    public static DXWorkflow getInstanceWithEnvironment(String workflowId, DXContainer project,
            DXEnvironment env) {
        return new DXWorkflow(workflowId, project, Preconditions.checkNotNull(env,
                "env may not be null"), null);
    }

    /**
     * Returns a {@code DXWorkflow} associated with an existing workflow using the specified
     * environment.
     *
     * @throws NullPointerException If {@code workflowId} is null
     */
    public static DXWorkflow getInstanceWithEnvironment(String workflowId, DXEnvironment env) {
        return new DXWorkflow(workflowId, Preconditions.checkNotNull(env, "env may not be null"));
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

    private DXWorkflow(String workflowId, DXContainer project, DXEnvironment env, JsonNode describe) {
        super(workflowId, "workflow", project, env, describe);

    }

    private DXWorkflow(String workflowId, DXEnvironment env) {
        super(workflowId, "workflow", env, null);
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

    @Override
    public ExecutableRunner<DXAnalysis> newRun() {
        return ExecutableRunner.getWorkflowRunnerWithEnvironment(this.getId(), this.env);
    }

    /**
     * A workflow stage.
     */
    public static class Stage {
        private String ID;

        Stage(String ID) {
            this.ID = ID;
        }

        public String getId() {
            return ID;
        }

        /**
         * Create a link to an output field.
         *
         * <p>This is used in workflows, to link results between stages.</p>
         *
         * @param outputName  name of an output field
         *
         * @return JSON representation of a link. Can be used as an input to a workflow stage.
         */
        public JsonNode getOutputReference(String  outputName) {
            ObjectNode dxlink = DXJSON.getObjectBuilder()
                .put("stage", ID)
                .put("outputField", outputName).build();
            return DXJSON.getObjectBuilder().put("$dnanexus_link", dxlink).build();
        }

        /**
         * Create a link to an input field.
         *
         * <p>This is used in workflows, to link results between stages.</p>
         *
         * @param inputName  name of an input field
         *
         * @return JSON representation of a link. Can be used as an input to a workflow stage.
         */
        public ObjectNode getInputReference(String  inputName) {
            ObjectNode dxlink = DXJSON.getObjectBuilder()
                .put("stage", ID)
                .put("inputField", inputName).build();
            return DXJSON.getObjectBuilder().put("$dnanexus_link", dxlink).build();
        }
    }

    /**
     * Represents the result of a workflow-modifying operation, along with the
     * workflow's edit version after that operation.
     */
    public static class Modification<T> {
        private final int editVersion;
        private final T obj;

        Modification(int editVersion, T obj) {
            this.editVersion = editVersion;
            this.obj = obj;
        }

        public int getEditVersion() {
            return editVersion;
        }

        public T getValue() {
            return obj;
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class WorkflowAddStageInput {
        @JsonProperty
        public int editVersion;

        @JsonProperty
        public String name;

        @JsonProperty
        private JsonNode input;

        @JsonProperty
        public String executable;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class WorkflowAddStageOutput {
        @JsonProperty
        public int editVersion;

        @JsonProperty
        public String stage;
    }

    /**
     * Adds a stage to a workflow.
     *
     * @param applet Applet to run
     * @param name   stage name
     * @param stageInputs  inputs to be provided to the applet
     * @param editVersion current version of the workflow
     *
     * @return Modification object containing the new stage
     */
    public Modification<Stage> addStage(DXApplet applet,
                                        String name,
                                        Object stageInputs,
                                        int editVersion) {
        WorkflowAddStageInput reqInput = new WorkflowAddStageInput();
        reqInput.editVersion = editVersion;
        reqInput.name = name;
        reqInput.input = MAPPER.valueToTree(stageInputs);
        reqInput.executable = applet.getId();
        WorkflowAddStageOutput reqOutput = DXAPI.workflowAddStage(this.getId(),
                                                                  reqInput, WorkflowAddStageOutput.class);
        return new Modification<Stage> (reqOutput.editVersion,
                                        new Stage(reqOutput.stage));
    }
}
