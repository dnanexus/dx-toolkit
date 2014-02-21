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
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;

/**
 * An execution (job or analysis).
 */
public abstract class DXExecution extends DXObject {

    /**
     * Contains metadata about an execution. All accessors reflect the state of the execution at the
     * time that this object was created.
     */
    public static abstract class Describe {

        // TODO: executableName
        // TODO: created
        // TODO: modified
        // TODO: billTo
        // TODO: project
        // TODO: folder
        // TODO: rootExecution
        // TODO: parentJob
        // TODO: parentAnalysis
        // TODO: analysis
        // TODO: stage
        // TODO: stages
        // TODO: workspace
        // TODO: launchedBy
        // TODO: details
        // TODO: runInput
        // TODO: originalInput
        // TODO: input
        // TODO: delayWorkspaceDestruction
        // TODO: totalPrice

        /**
         * Returns the ID of the execution.
         *
         * @return the execution ID
         */
        public abstract String getId();

        /**
         * Returns the name of the execution.
         *
         * @return the execution name
         */
        public abstract String getName();

        /**
         * Returns the output of the execution, deserialized to the specified class.
         *
         * <p>
         * Note that this field is not guaranteed to be complete until the execution has finished.
         * However, partial outputs might be populated before that time (in the case of a job, as
         * soon as the job has reached "waiting_on_output", or in the case of an analysis, as each
         * stage finishes). Note that until the execution has completely finished and reached the
         * "done" state, the output might contain job-based object references (which may require
         * special deserialization code in your target class).
         * </p>
         *
         * @param outputClass class to deserialize to
         *
         * @return output object
         */
        public abstract <T> T getOutput(Class<T> outputClass);

        /**
         * Returns the properties associated with the execution.
         *
         * @return Map of property keys and values
         */
        public abstract Map<String, String> getProperties();

        /**
         * Returns the tags associated with the execution.
         *
         * @return List of tags
         */
        public abstract List<String> getTags();

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
