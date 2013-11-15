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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A job object (a specific instantiation of an app or applet).
 */
public final class DXJob extends DXObject {

    /**
     * Contains metadata about a job. All accessors reflect the state of the job at the time that
     * this object was created.
     */
    public final static class Describe {
        private final DescribeResponseHash describeOutput;
        private final DXEnvironment env;

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
         * Returns the ID of the job.
         *
         * @return the job ID
         */
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

        /**
         * Returns the name of the job.
         *
         * @return the job name
         */
        public String getName() {
            return describeOutput.name;
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

        /**
         * Returns the state of the job.
         *
         * @return job state
         */
        public JobState getState() {
            return describeOutput.state;
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
    }

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
        Preconditions.checkNotNull(env);
        return new DXJob(jobId, env);
    }

    private DXJob(String jobId) {
        super(jobId, null);
    }

    private DXJob(String jobId, DXEnvironment env) {
        super(jobId, env);
    }

    /**
     * Obtains information about the job.
     *
     * @return a {@code Describe} containing job metadata
     */
    public Describe describe() {
        return new Describe(DXJSON.safeTreeToValue(DXAPI.jobDescribe(this.getId()),
                DescribeResponseHash.class), this.env);
    }

    /**
     * Terminates the job.
     */
    public void terminate() {
        DXAPI.jobTerminate(this.dxId);
    }

}
