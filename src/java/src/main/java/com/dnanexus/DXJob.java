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

import java.util.Date;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * A job object (a specific instantiation of an app or applet).
 */
public final class DXJob extends DXExecution {

    /**
     * Contains metadata about a job. All accessors reflect the state of the job at the time that
     * this object was created.
     */
    public final static class Describe extends DXExecution.Describe {

        private final DescribeResponseHash describeOutput;

        // TODO: systemRequirements
        // TODO: executionPolicy
        // TODO: instanceType
        // TODO: failureFrom, failureReports
        // TODO: provide accessors for dependsOn
        // TODO: provide accessor for app (there is no DXApp class right now)

        @VisibleForTesting
        Describe(DescribeResponseHash describeOutput, DXEnvironment env) {
            super(describeOutput, env);
            this.describeOutput = describeOutput;
        }

        /**
         * Returns the applet of the job if it is running an applet, or null otherwise.
         *
         * @return applet or null
         */
        public DXApplet getApplet() {
            if (describeOutput.applet == null) {
                return null;
            }
            return DXApplet.getInstanceWithEnvironment(describeOutput.applet, env);
        }

        /**
         * Returns a detailed message describing why the job failed, or null if the job is not in a
         * failing or failed state.
         *
         * @return detailed failure message, or null
         */
        public String getFailureMessage() {
            return describeOutput.failureMessage;
        }

        /**
         * Returns a short String describing why the job failed, or null if the job is not in a
         * failing or failed state.
         *
         * @return short failure reason, or null
         */
        public String getFailureReason() {
            return describeOutput.failureReason;
        }

        /**
         * Returns the name of the function (entry point) that the job is running.
         *
         * @return function name
         */
        public String getFunction() {
            return describeOutput.function;
        }

        /**
         * Returns the closest ancestor job whose parentJob is null. This is the nearest job that
         * was run by a user directly or was run as a stage in an analysis.
         *
         * @return origin job
         */
        public DXJob getOriginJob() {
            return DXJob.getInstanceWithEnvironment(describeOutput.originJob, env);
        }

        /**
         * Returns the output of the job, deserialized to the specified class, or null if no output
         * hash is available. Note that this field is not guaranteed to be complete until the job
         * has finished.
         *
         * <p>
         * A partial output hash is available as soon as the job reaches state "waiting_on_output".
         * At this time the output may contain unresolved job-based object references. These
         * references will be resolved by the time the job reaches state "done".
         * </p>
         *
         * @param outputClass class to deserialize to
         *
         * @return output object or null
         */
        @Override
        public <T> T getOutput(Class<T> outputClass) {
            // We don't do anything different here except for providing the job-specific
            // caveats in the docstring above.
            return super.getOutput(outputClass);
        }

        /**
         * Returns the project cache container for the job if it is running an app, or null
         * otherwise.
         *
         * @return project cache container or null
         */
        public DXContainer getProjectCache() {
            if (describeOutput.projectCache == null) {
                return null;
            }
            return DXContainer.getInstanceWithEnvironment(describeOutput.projectCache, env);
        }

        /**
         * Returns the resources container for the job if it is running an app, or null otherwise.
         *
         * @return resources container or null
         */
        public DXContainer getResources() {
            if (describeOutput.resources == null) {
                return null;
            }
            return DXContainer.getInstanceWithEnvironment(describeOutput.resources, env);
        }

        /**
         * Returns the date that the job started running, or null if the job has not started running
         * yet.
         *
         * @return start date or null
         */
        public Date getStartDate() {
            if (describeOutput.startedRunning == null) {
                return null;
            }
            return new Date(describeOutput.startedRunning);
        }

        /**
         * Returns the state of the job.
         *
         * @return job state
         */
        public JobState getState() {
            return describeOutput.state;
        }

        /**
         * Returns a list of the state transitions for the job, showing each state that the job
         * reached and at what time. Every job implicitly starts in state "idle" (which does not
         * appear in the state transition list).
         *
         * @return List of state transitions
         */
        public List<StateTransition> getStateTransitions() {
            return ImmutableList.copyOf(describeOutput.stateTransitions);
        }

        /**
         * Returns the date that the job stopped running, or null if the job has not stopped running
         * yet.
         *
         * @return stop date or null
         */
        public Date getStopDate() {
            if (describeOutput.stoppedRunning == null) {
                return null;
            }
            return new Date(describeOutput.stoppedRunning);
        }

        /**
         * Returns whether the job will be charged to the billed entity (billTo).
         *
         * <p>
         * Usually the job will be free if the job has failed for reasons (see
         * {@link #getFailureReason()}) that are generally indicative of some system error rather
         * than of user error.
         * </p>
         *
         * @return true if the job is free
         *
         * @throws IllegalStateException if the requesting user does not have permission to view the
         *         pricing model of the billed entity (billTo), or the price of the execution has
         *         not been finalized
         */
        public boolean isFree() {
            if (describeOutput.isFree == null) {
                throw new IllegalStateException("isFree is not available");
            }
            return describeOutput.isFree;
        }
    }

    /**
     * Deserialized output from the /job-xxxx/describe route. Not directly accessible by users (see
     * Describe instead).
     */
    @VisibleForTesting
    @JsonIgnoreProperties(ignoreUnknown = true)
    final static class DescribeResponseHash extends DXExecution.DescribeResponseHash {
        @JsonProperty
        private JobState state;
        @JsonProperty
        private Long startedRunning;
        @JsonProperty
        private Long stoppedRunning;
        @JsonProperty
        private String originJob;
        @JsonProperty
        private List<StateTransition> stateTransitions;
        @JsonProperty
        private String function;
        @JsonProperty
        private List<String> dependsOn;
        @JsonProperty
        private String failureReason;
        @JsonProperty
        private String failureMessage;
        @JsonProperty
        private Boolean isFree;
        @JsonProperty
        private String applet;
        @JsonProperty
        private String app;
        @JsonProperty
        private String resources;
        @JsonProperty
        private String projectCache;
    }

    /**
     * A response from the /job-xxxx/terminate route.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class JobTerminateResponse {}

    /**
     * A event where a job transitioned from one state to another.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public final static class StateTransition {
        @JsonProperty
        private JobState newState;
        @JsonProperty
        private Long setAt;

        @SuppressWarnings("unused")
        private StateTransition() {
            // No-arg constructor for JSON deserialization
        }

        @VisibleForTesting
        StateTransition(JobState newState, long setAt) {
            this.newState = newState;
            this.setAt = setAt;
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
            if (!(obj instanceof StateTransition)) {
                return false;
            }
            StateTransition other = (StateTransition) obj;
            if (newState != other.newState) {
                return false;
            }
            if (setAt == null) {
                if (other.setAt != null) {
                    return false;
                }
            } else if (!setAt.equals(other.setAt)) {
                return false;
            }
            return true;
        }

        /**
         * Returns the state of the job after the transition.
         *
         * @return new job state
         */
        public JobState getNewState() {
            return newState;
        }

        /**
         * Returns the date at which the new state became effective.
         *
         * @return transition time
         */
        public Date getSetAt() {
            return new Date(setAt);
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(newState, setAt);
        }
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
        super(jobId, "job", null);
    }

    private DXJob(String jobId, DXEnvironment env) {
        super(jobId, "job", env);
    }

    private DXJob(String jobId, DXEnvironment env, JsonNode cachedDescribe) {
        super(jobId, "job", env, cachedDescribe);
    }

    @Override
    public Describe describe() {
        return describeImpl(MAPPER.createObjectNode());
    }

    private Describe describeImpl(JsonNode describeInput) {
        return new Describe(DXAPI.jobDescribe(this.getId(), describeInput,
                DescribeResponseHash.class, this.env), this.env);
    }

    @Override
    public Describe getCachedDescribe() {
        this.checkCachedDescribeAvailable();
        return new Describe(
                DXJSON.safeTreeToValue(this.cachedDescribe, DescribeResponseHash.class), this.env);
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
