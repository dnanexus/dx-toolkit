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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * A job object (a specific instantiation of an app or applet).
 */
public final class DXJob extends DXExecution {

    /**
     * A response from the /job-xxxx/terminate route.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class JobTerminateResponse {}

    /**
     * Contains metadata about a job. All accessors reflect the state of the job at the time that
     * this object was created.
     */
    public final static class Describe extends DXExecution.Describe {
        private final DescribeResponseHash describeOutput;
        private final DXEnvironment env;

        // TODO: startedRunning
        // TODO: stoppedRunning
        // TODO: originJob
        // TODO: stateTransitions
        // TODO: function
        // TODO: tags
        // TODO: properties
        // TODO: systemRequirements
        // TODO: executionPolicy
        // TODO: instanceType
        // TODO: dependsOn
        // TODO: failure*
        // TODO: isFree
        // TODO: applet/app
        // TODO: resources
        // TODO: projectCache

        // TODO: plus the common fields in DXExecution.Describe

        @VisibleForTesting
        Describe(DescribeResponseHash describeOutput, DXEnvironment env) {
            this.describeOutput = describeOutput;
            this.env = env;
        }

        /**
         * Returns the date at which the job was created.
         *
         * @return the job's creation date
         */
        public Date getCreationDate() {
            return new Date(describeOutput.created);
        }

        /**
         * Returns the details (user-suppled metadata) of the job.
         *
         * @param valueType class to deserialize to
         *
         * @return the job's details
         */
        public <T> T getDetails(Class<T> valueType) {
            return DXJSON.safeTreeToValue(describeOutput.details, valueType);
        }

        @Override
        public String getId() {
            return describeOutput.id;
        }

        /**
         * Returns the date at which the job was last modified.
         *
         * @return the job's modification date
         */
        public Date getModifiedDate() {
            return new Date(describeOutput.modified);
        }

        @Override
        public String getName() {
            return describeOutput.name;
        }

        @Override
        public <T> T getOutput(Class<T> outputClass) {
            return DXJSON.safeTreeToValue(describeOutput.output, outputClass);
        }

        /**
         * Returns the job's parent job, or {@code null} if the job is an origin job.
         *
         * @return {@code DXJob} for parent job
         */
        public DXJob getParentJob() {
            if (describeOutput.parentJob == null) {
                return null;
            }
            return new DXJob(describeOutput.parentJob, env);
        }

        @Override
        public Map<String, String> getProperties() {
            return ImmutableMap.copyOf(describeOutput.properties);
        }

        /**
         * Returns the state of the job.
         *
         * @return job state
         */
        public JobState getState() {
            return describeOutput.state;
        }

        @Override
        public List<String> getTags() {
            return ImmutableList.copyOf(describeOutput.tags);
        }
    }

    /**
     * Deserialized output from the /job-xxxx/describe route. Not directly accessible by users (see
     * Describe instead).
     */
    @VisibleForTesting
    @JsonIgnoreProperties(ignoreUnknown = true)
    final static class DescribeResponseHash {
        @JsonProperty
        private String id;
        @JsonProperty
        private String name;
        @JsonProperty
        private Long created;
        @JsonProperty
        private Long modified;
        @JsonProperty
        private String parentJob;
        @JsonProperty
        private JobState state;
        @JsonProperty
        private JsonNode details;
        @JsonProperty
        private List<String> tags;
        @JsonProperty
        private Map<String, String> properties;

        @JsonProperty
        private JsonNode output;
    }

    private static final Set<JobState> unsuccessfulJobStates = Sets.immutableEnumSet(
            JobState.FAILED, JobState.TERMINATED);

    /**
     * Returns a {@code DXJob} representing the specified job.
     *
     * @param jobId Job ID, of the form {@code "job-xxxx"}
     *
     * @return a {@code DXJob}
     *
     * @throws NullPointerException If {@code jobId} is null
     */
    public static DXJob getInstance(String jobId) {
        return new DXJob(jobId);
    }

    /**
     * Returns a {@code DXJob} representing the specified job using the specified environment, with
     * the specified cached describe output.
     *
     * <p>
     * This method is for use exclusively by bindings to the "find" routes when describe hashes are
     * returned with the find output.
     * </p>
     *
     * @throws NullPointerException If any argument is null
     */
    static DXJob getInstanceWithCachedDescribe(String jobId, DXEnvironment env, JsonNode describe) {
        return new DXJob(jobId, Preconditions.checkNotNull(env, "env may not be null"),
                Preconditions.checkNotNull(describe, "describe may not be null"));
    }

    /**
     * Returns a {@code DXJob} representing the specified job using the specified environment.
     *
     * @param jobId Job ID, of the form {@code "job-xxxx"}
     * @param env environment to use for making subsequent API calls
     *
     * @return a {@code DXJob}
     *
     * @throws NullPointerException If {@code jobId} or {@code env} is null
     */
    public static DXJob getInstanceWithEnvironment(String jobId, DXEnvironment env) {
        return new DXJob(jobId, Preconditions.checkNotNull(env, "env may not be null"));
    }

    private DXJob(String jobId) {
        super(jobId, null);
    }

    private DXJob(String jobId, DXEnvironment env) {
        super(jobId, env);
    }

    private DXJob(String jobId, DXEnvironment env, JsonNode cachedDescribe) {
        super(jobId, env, cachedDescribe);
    }

    private Describe describeImpl(JsonNode describeInput) {
        return new Describe(DXAPI.jobDescribe(this.getId(), describeInput,
                DescribeResponseHash.class), this.env);
    }

    @Override
    public Describe describe() {
        return describeImpl(MAPPER.createObjectNode());
    }

    @Override
    public Describe getCachedDescribe() {
        this.checkCachedDescribeAvailable();
        return new Describe(DXJSON.safeTreeToValue(this.cachedDescribe, DescribeResponseHash.class), this.env);
    }

    @Override
    public <T> T getOutput(Class<T> outputClass) throws IllegalStateException {
        // {fields: {output: true, state: true}}
        Describe d =
                describeImpl(DXJSON
                        .getObjectBuilder()
                        .put("fields",
                                DXJSON.getObjectBuilder().put("output", true).put("state", true)
                                        .build()).build());
        if (d.getState() != JobState.DONE) {
            throw new IllegalStateException("Expected job to be in state DONE, but it is in state "
                    + d.getState());
        }
        return d.getOutput(outputClass);
    }

    @Override
    public void terminate() {
        DXAPI.jobTerminate(this.getId(), JobTerminateResponse.class);
    }

    /**
     * Waits until the job has successfully completed and is in the DONE state.
     *
     * @return the same DXJob object
     *
     * @throws IllegalStateException if the job reaches the FAILED or TERMINATED state
     */
    @Override
    public DXJob waitUntilDone() throws IllegalStateException {
        JobState jobState = this.describe().getState();
        while (jobState != JobState.DONE) {
            if (unsuccessfulJobStates.contains(jobState)) {
                throw new IllegalStateException(this.getId() + " is in unsuccessful state "
                        + jobState.toString());
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            jobState = this.describe().getState();
        }
        return this;
    }

}
