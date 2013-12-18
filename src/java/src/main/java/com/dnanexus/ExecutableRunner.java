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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Collects parameters for a new run of an executable. To obtain an instance, call
 * {@link DXExecutable#newRun()}.
 *
 * @param <T> the type of the execution that will be produced
 */
public abstract class ExecutableRunner<T extends DXExecution> {

    /**
     * Concrete subclass for running an applet, yielding a job.
     */
    private static class AppletRunner extends ExecutableRunner<DXJob> {
        private AppletRunner(String appletId, DXEnvironment env) {
            super(appletId, env);
            Preconditions.checkArgument(appletId.startsWith("applet-"),
                    "Applet ID must start with applet-");
        }

        /*
         * (non-Javadoc)
         *
         * @see com.dnanexus.ExecutableRunner#run()
         */
        @Override
        public DXJob run() {
            ExecutableRunResult runResult =
                    DXJSON.safeTreeToValue(new DXHTTPRequest(env).request("/" + this.executableId
                            + "/run", buildRequestHash()), ExecutableRunResult.class);
            return DXJob.getInstanceWithEnvironment(runResult.id, env);
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class ExecutableRunRequest {

        @SuppressWarnings("unused")
        @JsonProperty
        private final JsonNode input;
        @SuppressWarnings("unused")
        @JsonProperty
        private final String name;
        @SuppressWarnings("unused")
        @JsonProperty
        private final List<String> dependsOn;
        @SuppressWarnings("unused")
        @JsonProperty
        private final String project;
        @SuppressWarnings("unused")
        @JsonProperty
        private final String folder;
        @SuppressWarnings("unused")
        @JsonProperty
        private final Boolean delayWorkspaceDestruction;
        @SuppressWarnings("unused")
        @JsonProperty
        private final JsonNode details;

        // TODO: systemRequirements

        // TODO: tags
        // TODO: properties
        // TODO: executionPolicy

        public ExecutableRunRequest(ExecutableRunner<?> runner) {
            this.input = runner.input;
            this.name = runner.name;
            this.dependsOn = runner.getDependencies();
            this.project = runner.project;
            this.folder = runner.folder;
            this.delayWorkspaceDestruction = runner.delayWorkspaceDestruction;
            this.details = runner.details;
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ExecutableRunResult {
        @JsonProperty
        private String id;
    }

    /**
     * Concrete subclass for running an workflow, yielding an analysis.
     */
    private static class WorkflowRunner extends ExecutableRunner<DXAnalysis> {
        private WorkflowRunner(String workflowId, DXEnvironment env) {
            super(workflowId, env);
            Preconditions.checkArgument(workflowId.startsWith("workflow-"),
                    "Workflow ID must start with workflow-");
        }

        /*
         * (non-Javadoc)
         *
         * @see com.dnanexus.ExecutableRunner#run()
         */
        @Override
        public DXAnalysis run() {
            ExecutableRunResult runResult =
                    DXJSON.safeTreeToValue(new DXHTTPRequest(env).request("/" + this.executableId
                            + "/run", buildRequestHash()), ExecutableRunResult.class);
            return DXAnalysis.getInstanceWithEnvironment(runResult.id, env);
        }
    }

    /**
     * Returns a runner that will run the given applet with the given environment. To obtain an
     * instance, use {@link DXApplet#newRun()}.
     *
     * @param appletId applet ID
     * @param env environment to be used to make API requests
     *
     * @return an executable runner
     */
    static ExecutableRunner<DXJob> getAppletRunnerWithEnvironment(String appletId, DXEnvironment env) {
        return new AppletRunner(appletId, env);
    }

    /**
     * Returns a runner that will run the given workflow with the given environment. To obtain an
     * instance, use {@link DXWorkflow#newRun()}.
     *
     * @param workflowId workflow ID
     * @param env environment to be used to make API requests
     *
     * @return an executable runner
     */
    static ExecutableRunner<DXAnalysis> getWorkflowRunnerWithEnvironment(String workflowId,
            DXEnvironment env) {
        return new WorkflowRunner(workflowId, env);
    }

    protected final String executableId;
    protected final DXEnvironment env;

    private JsonNode input;
    private String name;
    private String project;
    private String folder;
    private Boolean delayWorkspaceDestruction;
    private JsonNode details;

    private List<DXJob> jobDependencies = Lists.newArrayList();

    private List<DXDataObject> objectDependencies = Lists.newArrayList();

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Initializes a runner that will run the given executable with the given environment.
     *
     * @param executableId executable ID, e.g. "applet-xxxx", "app-xxxx", or "app-name/version"
     * @param env environment to be used to make API requests
     */
    private ExecutableRunner(String executableId, DXEnvironment env) {
        Preconditions.checkNotNull(executableId, "executable ID may not be null");
        Preconditions.checkNotNull(env, "environment may not be null");
        this.executableId = executableId;
        this.env = env;
    }

    /**
     * Returns the hash that would be provided as input to the executable-xxxx/run method.
     *
     * @return JSON hash
     */
    @VisibleForTesting
    JsonNode buildRequestHash() {
        ExecutableRunRequest runRequest = new ExecutableRunRequest(this);
        return MAPPER.valueToTree(runRequest);
    }

    /**
     * Delays the destruction of the workspace of the resulting job so it can be examined for
     * debugging.
     *
     * @return the same runner object
     */
    public ExecutableRunner<T> delayWorkspaceDestruction() {
        Preconditions.checkState(this.delayWorkspaceDestruction == null,
                "delayWorkspaceDestruction cannot be called more than once");
        this.delayWorkspaceDestruction = true;
        return this;
    }

    /**
     * Makes the resulting job depend on the specified data object, so that the job will not begin
     * until the data object is closed.
     *
     * @param dataObject data object to depend on
     *
     * @return the same runner object
     */
    public ExecutableRunner<T> dependsOn(DXDataObject dataObject) {
        Preconditions.checkNotNull(dataObject, "data object may not be null");
        this.objectDependencies.add(dataObject);
        return this;
    }

    /**
     * Makes the resulting job depend on the specified job, so that the former will not begin until
     * the latter has successfully completed.
     *
     * @param job job to depend on
     *
     * @return the same runner object
     */
    public ExecutableRunner<T> dependsOn(DXJob job) {
        Preconditions.checkNotNull(job, "job may not be null");
        this.jobDependencies.add(job);
        return this;
    }

    private List<String> getDependencies() {
        if (jobDependencies.size() == 0 && objectDependencies.size() == 0) {
            return null;
        }
        List<String> result = Lists.newArrayList();
        for (DXJob job : jobDependencies) {
            result.add(job.getId());
        }
        for (DXDataObject object : objectDependencies) {
            result.add(object.getId());
        }
        return result;
    }

    /**
     * Sets the folder in which the job outputs will be deposited.
     *
     * @param folder full path to folder (a String starting with "/")
     *
     * @return the same runner object
     */
    public ExecutableRunner<T> inFolder(String folder) {
        Preconditions.checkState(this.folder == null, "inFolder cannot be called more than once");
        Preconditions.checkArgument(folder != null, "folder may not be null");
        this.folder = folder;
        return this;
    }

    /**
     * Sets the project context of the resulting job.
     *
     * @param project project in which the executable will be run
     *
     * @return the same runner object
     */
    public ExecutableRunner<T> inProject(DXProject project) {
        Preconditions.checkState(this.project == null, "inProject cannot be called more than once");
        if (project == null) {
            throw new NullPointerException("project may not be null");
        }
        this.project = project.getId();
        return this;
    }

    /**
     * Runs the executable.
     *
     * @return the resulting execution object
     */
    public abstract T run();

    /**
     * Sets the job details to the JSON serialized value of the specified object.
     *
     * @param details user-supplied metadata
     *
     * @return the same runner object
     */
    public ExecutableRunner<T> withDetails(Object details) {
        Preconditions.checkState(this.details == null,
                "withDetails cannot be called more than once");
        JsonNode serializedDetails = MAPPER.valueToTree(details);
        Preconditions.checkNotNull(serializedDetails, "details may not serialize to null");
        Preconditions.checkArgument(serializedDetails.isArray() || serializedDetails.isObject(),
                "details must serialize to an object or array");
        this.details = serializedDetails;
        return this;
    }

    /**
     * Sets the input hash to the JSON serialized value of the specified object.
     *
     * @param inputObject object to be JSON serialized
     *
     * @return the same runner object
     */
    public ExecutableRunner<T> withInput(Object inputObject) {
        return withRawInput(MAPPER.valueToTree(inputObject));
    }

    /**
     * Sets the name of the resulting job.
     *
     * @param name job name
     *
     * @return the same runner object
     */
    public ExecutableRunner<T> withName(String name) {
        Preconditions.checkState(this.name == null, "withName cannot be called more than once");
        Preconditions.checkArgument(name != null, "name may not be null");
        this.name = name;
        return this;
    }

    /**
     * Sets the input hash to the specified JSON node.
     *
     * @param inputHash
     *
     * @return the same runner object
     */
    public ExecutableRunner<T> withRawInput(JsonNode inputHash) {
        Preconditions.checkState(this.input == null,
                "withInput or withRawInput cannot be called more than once");
        Preconditions.checkArgument(inputHash != null, "input hash may not be null");
        this.input = inputHash;
        return this;
    }
}
