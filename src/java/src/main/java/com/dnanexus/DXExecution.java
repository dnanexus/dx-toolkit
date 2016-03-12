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

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * An execution (job or analysis).
 */
public abstract class DXExecution extends DXObject {

    /**
     * Contains metadata about an execution. All accessors reflect the state of the execution at the
     * time that this object was created.
     */
    public static abstract class Describe {

        private DescribeResponseHash describeOutput;
        protected DXEnvironment env;

        @VisibleForTesting
        Describe(DescribeResponseHash describeOutput, DXEnvironment env) {
            this.describeOutput = describeOutput;
            this.env = env;
        }

        /**
         * Returns the analysis that this execution was run as a stage of, or null if this execution
         * was not run as a stage in an analysis.
         *
         * @return immediate parent analysis, or null
         */
        public DXAnalysis getAnalysis() {
            if (describeOutput.analysis == null) {
                return null;
            }
            return DXAnalysis.getInstanceWithEnvironment(describeOutput.analysis, env);
        }

        /**
         * Returns the user or organization that this execution was billed to.
         *
         * @return user or organization ID
         */
        public String getBillTo() {
            return describeOutput.billTo;
        }

        /**
         * Returns the date at which the execution was created.
         *
         * @return the execution's creation date
         */
        public Date getCreationDate() {
            return new Date(describeOutput.created);
        }

        /**
         * Returns the details (user-suppled metadata) of the execution, deserialized to the
         * specified class.
         *
         * @param detailsClass class to deserialize to
         *
         * @return the execution's details
         */
        public <T> T getDetails(Class<T> detailsClass) {
            return DXJSON.safeTreeToValue(describeOutput.details, detailsClass);
        }

        /**
         * Returns the name of the executable that was run to create this execution.
         *
         * @return name of applet, app, or workflow
         */
        public String getExecutableName() {
            return describeOutput.executableName;
        }

        /**
         * Returns the output folder of the execution.
         *
         * @return full path (beginning with "/") of the folder in which the outputs of the
         *         execution will be placed
         */
        public String getFolder() {
            return describeOutput.folder;
        }

        /**
         * Returns the ID of the execution.
         *
         * @return the execution ID
         */
        public String getId() {
            return describeOutput.id;
        }

        /**
         * Returns the execution's input as it will be given to the job (to the extent possible),
         * deserialized to the specified class. This is the same as the output of
         * {@link #getOriginalInput(Class)}, except that as job-based object references are resolved
         * they are replaced with the resulting object IDs; this does not apply to analyses, for
         * which the result of this method is always the same as that of
         * {@link #getOriginalInput(Class)}. Once a job has transitioned to state "runnable", this
         * is exactly the input that the job will receive.
         *
         * <p>
         * Note that for jobs, this input may contain job-based object references (until the time
         * that the job enters the "runnable" state), which may require special deserialization code
         * in your target class.
         * </p>
         *
         * @param inputClass class to deserialize to
         *
         * @return execution's input
         */
        public <T> T getInput(Class<T> inputClass) {
            if (describeOutput.input == null) {
                throw new IllegalStateException(
                        "input is not available because it was not retrieved with the describe call");
            }
            return DXJSON.safeTreeToValue(describeOutput.input, inputClass);
        }

        /**
         * Returns the user that launched this execution.
         *
         * @return user ID
         */
        public String getLaunchedBy() {
            return describeOutput.launchedBy;
        }

        /**
         * Returns the date at which the execution was last modified.
         *
         * @return the execution's modification date
         */
        public Date getModifiedDate() {
            return new Date(describeOutput.modified);
        }

        /**
         * Returns the name of the execution.
         *
         * @return the execution name
         */
        public String getName() {
            return describeOutput.name;
        }

        /**
         * Returns the input of the execution, with default values filled in for optional inputs
         * that were not provided, deserialized to the specified class. For analyses, all input
         * field names will also have been canonicalized to the form "stage-XXXX.fieldName".
         *
         * <p>
         * Note that this input may contain job-based object references, which may require special
         * deserialization code in your target class.
         * </p>
         *
         * @param inputClass class to deserialize to
         *
         * @return execution's input with default values filled in
         */
        public <T> T getOriginalInput(Class<T> inputClass) {
            if (describeOutput.originalInput == null) {
                throw new IllegalStateException(
                        "original input is not available because it was not retrieved with the describe call");
            }
            return DXJSON.safeTreeToValue(describeOutput.originalInput, inputClass);
        }

        /**
         * Returns the output of the execution, deserialized to the specified class, or null if no
         * output hash is available. Note that this field is not guaranteed to be complete until the
         * execution has finished.
         *
         * @param outputClass class to deserialize to
         *
         * @return output object or null
         */
        public <T> T getOutput(Class<T> outputClass) {
            // TODO: the same treatment here with IllegalStateException should be given to all
            // fields, although the bindings do not yet allow supplying describe options that would
            // turn off those fields.
            if (describeOutput.output == null) {
                throw new IllegalStateException(
                        "output is not available because it was not retrieved with the describe call");
            }
            if (describeOutput.output.isNull()) {
                return null;
            }
            return DXJSON.safeTreeToValue(describeOutput.output, outputClass);
        }

        /**
         * Returns the execution's parent analysis, or {@code null} if the execution was not created
         * by a analysis.
         *
         * @return parent analysis or null
         */
        public DXAnalysis getParentAnalysis() {
            if (describeOutput.parentAnalysis == null) {
                return null;
            }
            return DXAnalysis.getInstanceWithEnvironment(describeOutput.parentAnalysis, env);
        }

        /**
         * Returns the execution's parent job, or {@code null} if the execution was not created by a
         * job.
         *
         * @return parent job or null
         */
        public DXJob getParentJob() {
            if (describeOutput.parentJob == null) {
                return null;
            }
            return DXJob.getInstanceWithEnvironment(describeOutput.parentJob, env);
        }

        /**
         * Returns the project in which this execution was run.
         *
         * @return project
         */
        public DXProject getProject() {
            return DXProject.getInstanceWithEnvironment(describeOutput.project, env);
        }

        /**
         * Returns the properties associated with the execution.
         *
         * @return Map of property keys and values
         */
        public Map<String, String> getProperties() {
            return ImmutableMap.copyOf(describeOutput.properties);
        }

        /**
         * Returns the execution at the root of the execution tree (the job or analysis that was
         * created by a user's external API call rather than by a job or run as a stage in an
         * analysis).
         *
         * @return root execution
         */
        public DXExecution getRootExecution() {
            return DXExecution.getInstanceWithEnvironment(describeOutput.rootExecution, env);
        }

        /**
         * Returns the raw input of the execution, deserialized to the specified class. This is the
         * input as supplied to the executable "run" call.
         *
         * <p>
         * Note that this input may contain job-based object references, which may require special
         * deserialization code in your target class.
         * </p>
         *
         * @param inputClass class to deserialize to
         *
         * @return execution's input as supplied to the "run" call
         */
        public <T> T getRunInput(Class<T> inputClass) {
            if (describeOutput.runInput == null) {
                throw new IllegalStateException(
                        "run input is not available because it was not retrieved with the describe call");
            }
            return DXJSON.safeTreeToValue(describeOutput.runInput, inputClass);
        }

        /**
         * Returns the stage that this execution is associated with, if it was run as a stage in an
         * analysis; null otherwise.
         *
         * @return stage ID
         */
        public String getStage() {
            return describeOutput.stage;
        }

        /**
         * Returns the tags associated with the execution.
         *
         * @return List of tags
         */
        public List<String> getTags() {
            return ImmutableList.copyOf(describeOutput.tags);
        }

        /**
         * Returns the total price of the job.
         *
         * @return price in dollars
         *
         * @throws IllegalStateException if the requesting user does not have permission to view the
         *         pricing model of the billed entity (billTo), or the price of the execution has
         *         not been finalized
         */
        public double getTotalPrice() {
            if (describeOutput.totalPrice == null) {
                throw new IllegalStateException("total price is not available");
            }
            return describeOutput.totalPrice;
        }

        /**
         * Returns the temporary workspace associated with the execution.
         *
         * @return temporary workspace
         */
        public DXContainer getWorkspace() {
            return DXContainer.getInstanceWithEnvironment(describeOutput.workspace, env);
        }

        /**
         * Returns true if the execution's temporary workspace will be kept around for 3 days after
         * the execution finishes.
         *
         * @return true if the workspace destruction will be delayed
         *
         * @throws IllegalStateException if the execution is a job and is neither an origin job nor
         *         master job
         */
        public boolean isWorkspaceDestructionDelayed() {
            if (describeOutput.delayWorkspaceDestruction == null) {
                throw new IllegalStateException("delayWorkspaceDestruction is not available");
            }
            return describeOutput.delayWorkspaceDestruction;
        }

    }

    /**
     * Deserialized output from the /analysis-xxxx/describe or /job-xxxx/describe route (fields
     * common to all executions only).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    protected abstract static class DescribeResponseHash {
        @JsonProperty
        protected String id;
        @JsonProperty
        protected String name;
        @JsonProperty
        protected Long created;
        @JsonProperty
        protected Long modified;
        @JsonProperty
        protected JsonNode details;
        @JsonProperty
        protected String project;
        @JsonProperty
        protected String folder;
        @JsonProperty
        private List<String> tags;
        @JsonProperty
        private Map<String, String> properties;

        @JsonProperty
        protected String executableName;
        @JsonProperty
        protected String workspace;
        @JsonProperty
        protected String billTo;
        @JsonProperty
        protected String launchedBy;
        @JsonProperty
        protected Double totalPrice;
        @JsonProperty
        protected Boolean delayWorkspaceDestruction;

        @JsonProperty
        protected String analysis;
        @JsonProperty
        protected String stage;
        @JsonProperty
        protected String rootExecution;
        @JsonProperty
        protected String parentAnalysis;
        @JsonProperty
        protected String parentJob;

        @JsonProperty
        protected JsonNode input;
        @JsonProperty
        protected JsonNode runInput;
        @JsonProperty
        protected JsonNode originalInput;
        @JsonProperty
        protected JsonNode output;
    }

    /**
     * Returns a {@code DXExecution} corresponding to an existing execution with the specified ID.
     *
     * @param executionId DNAnexus execution id
     *
     * @return a {@code DXExecution} handle to the specified object
     */
    public static DXExecution getInstance(String executionId) {
        if (executionId.startsWith("job-")) {
            return DXJob.getInstance(executionId);
        } else if (executionId.startsWith("analysis-")) {
            return DXAnalysis.getInstance(executionId);
        }
        throw new IllegalArgumentException("The object ID " + executionId
                + " was of an unrecognized or unsupported class.");
    }

    /**
     * Returns a {@code DXExecution} corresponding to an existing execution with the specified ID,
     * using the specified environment, and with the specified cached describe data.
     *
     * @param executionId DNAnexus execution id
     * @param env environment to use to make subsequent API requests
     * @param cachedDescribe JSON hash of the describe output for this object
     *
     * @return a {@code DXExecution} handle to the specified object
     */
    static DXExecution getInstanceWithCachedDescribe(String executionId, DXEnvironment env,
            JsonNode cachedDescribe) {
        if (executionId.startsWith("job-")) {
            return DXJob.getInstanceWithCachedDescribe(executionId, env, cachedDescribe);
        } else if (executionId.startsWith("analysis-")) {
            return DXAnalysis.getInstanceWithCachedDescribe(executionId, env, cachedDescribe);
        }
        throw new IllegalArgumentException("The object ID " + executionId
                + " was of an unrecognized or unsupported class.");
    }

    /**
     * Returns a {@code DXExecution} corresponding to an existing execution with the specified ID,
     * using the specified environment.
     *
     * @param executionId DNAnexus execution id
     * @param env environment to use to make subsequent API requests
     *
     * @return a {@code DXExecution} handle to the specified object
     */
    public static DXExecution getInstanceWithEnvironment(String executionId, DXEnvironment env) {
        if (executionId.startsWith("job-")) {
            return DXJob.getInstanceWithEnvironment(executionId, env);
        } else if (executionId.startsWith("analysis-")) {
            return DXAnalysis.getInstanceWithEnvironment(executionId, env);
        }
        throw new IllegalArgumentException("The object ID " + executionId
                + " was of an unrecognized or unsupported class.");
    }

    protected final JsonNode cachedDescribe;

    /**
     * Initializes a new execution with the specified execution ID and environment.
     *
     * @param className class name that should prefix the ID
     */
    protected DXExecution(String dxId, String className, DXEnvironment env) {
        this(dxId, Preconditions.checkNotNull(className, "className may not be null"), env, null);
    }

    /**
     * Initializes a new execution with the specified execution ID, environment, and cached describe
     * data.
     *
     * @param className class name that should prefix the ID
     */
    protected DXExecution(String dxId, String className, DXEnvironment env, JsonNode cachedDescribe) {
        super(dxId, Preconditions.checkNotNull(className, "className may not be null"), env);
        this.cachedDescribe = cachedDescribe;
    }

    /**
     * Verifies that this object carries cached describe data.
     *
     * @throws IllegalStateException if cachedDescribe is not set
     */
    protected void checkCachedDescribeAvailable() throws IllegalStateException {
        if (this.cachedDescribe == null) {
            throw new IllegalStateException("This object contains no cached describe data.");
        }
    }

    /**
     * Obtains metadata about the execution.
     *
     * @return a {@code Describe} containing execution metadata
     */
    public abstract Describe describe();

    /**
     * Returns metadata about the data object, like {@link #describe()}, but without making an API
     * call.
     *
     * <p>
     * This cached describe info is only available if this object appears in the result of a
     * {@link DXSearch#findExecutions()} call that specified
     * {@link DXSearch.FindExecutionsRequestBuilder#includeDescribeOutput()}. The describe info that
     * is returned reflects the state of the object at the time that the search was performed.
     * </p>
     *
     * @return a {@code Describe} containing the execution's metadata
     *
     * @throws IllegalStateException if no cached describe info is available
     */
    public abstract Describe getCachedDescribe();

    /**
     * Returns the output of the execution, deserialized to the specified class.
     *
     * @param outputClass class to deserialize to
     *
     * @return execution output
     * @throws IllegalStateException if the execution is not in the DONE state.
     */
    public abstract <T> T getOutput(Class<T> outputClass) throws IllegalStateException;

    /**
     * Terminates the execution.
     */
    public abstract void terminate();

    /**
     * Waits until the execution has successfully completed and is in the DONE state.
     *
     * @return the same DXExecution object
     *
     * @throws IllegalStateException if the execution reaches the FAILED or TERMINATED state.
     */
    public abstract DXExecution waitUntilDone() throws IllegalStateException;

}
