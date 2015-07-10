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

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A project (a container providing features for data sharing and collaboration).
 */
public class DXProject extends DXContainer {

    private static void checkProjectId(String projectId) {
        Preconditions.checkArgument(projectId.startsWith("project-"), "Project ID " + projectId
                + " must start with project-");
    }

    /**
     * Contains metadata for a project. All accessors reflect the state of the project at the time
     * that this object was created.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Describe {
        @JsonProperty
        private String id;
        @JsonProperty
        private String name;

        /**
         * Creates a {@code Describe} object with all empty metadata.
         */
        private Describe() {}

        // TODO: other project metadata fields

        /**
         * Returns the name of the project.
         *
         * @return the name of the project
         */
        public String getName() {
            return this.name;
        }

    }

    /**
     * Builder class for creating a new {@code DXProject} object. To obtain an instance, call
     * {@link DXProject#newProject()}.
     */
    public static class Builder {

        private String name = null;
        private final DXEnvironment env;

        private Builder() {
            this.env = DXEnvironment.create();
        }

        private Builder(DXEnvironment env) {
            this.env = env;
        }

        /**
         * Sets the name of the new project.
         *
         * @param name String containing the new name of the project
         *
         * @return the same {@code Builder} object
         */
        public Builder setName(String name) {
            Preconditions.checkState(this.name == null, "Name may not be specified more than once");
            Preconditions.checkNotNull(name, "Name must be specified");
            Preconditions.checkArgument(name.length() > 0, "Name must be non-empty");
            this.name = name;
            return this;
        }

        @VisibleForTesting
        JsonNode buildRequestHash() {
            // Use this method to test the JSON hash created by a particular
            // builder call without actually executing the request.
            Preconditions.checkState(this.name != null, "name must be specified");
            return MAPPER.valueToTree(new ProjectNewRequest(this));
        }

        /**
         * Creates the project.
         *
         * @return a {@code DXProject} corresponding to the newly created project
         */
        public DXProject build() {
            return new DXProject(DXAPI.projectNew(this.buildRequestHash(), ObjectNewResponse.class,
                    env).getId(), env);
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class ProjectNewRequest {
        @JsonProperty
        private String name;

        private ProjectNewRequest(Builder builder) {
            this.name = builder.name;
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class ProjectTerminateRequest {
        @JsonProperty
        private boolean terminateJobs;

        private ProjectTerminateRequest(boolean terminateJobs) {
            this.terminateJobs = terminateJobs;
        }
    }

    private DXProject(String projectId) {
        super(projectId, null);
        checkProjectId(projectId);
    }

    private DXProject(String projectId, DXEnvironment env) {
        super(projectId, env);
        checkProjectId(projectId);
    }

    /**
     * Returns a {@code DXProject} associated with an existing project.
     *
     * @throws NullPointerException if {@code projectId} is null
     */
    public static DXProject getInstance(String projectId) {
        return new DXProject(projectId);
    }

    /**
     * Returns a {@code DXProject} associated with an existing project using the specified
     * environment.
     *
     * @throws NullPointerException if {@code projectId} or {@code env} is null
     */
    public static DXProject getInstanceWithEnvironment(String projectId, DXEnvironment env) {
        return new DXProject(projectId, Preconditions.checkNotNull(env, "env may not be null"));
    }

    /**
     * Returns a Builder object for creating a new {@code DXProject}.
     *
     * <p>
     * Example use:
     * </p>
     *
     * <pre>
     * DXProject r = DXProject.newProject().setName(&quot;foo&quot;).build();
     * </pre>
     *
     * @return a newly initialized {@code Builder}
     */
    public static Builder newProject() {
        return new Builder();
    }

    /**
     * Returns a Builder object for creating a new {@code DXProject} using the specified
     * environment.
     *
     * @return a newly initialized {@code Builder}
     */
    public static Builder newProjectWithEnvironment(DXEnvironment env) {
        return new Builder(env);
    }

    /**
     * Returns metadata about the project.
     *
     * @return a {@code Describe} object containing metadata
     */
    public Describe describe() {
        return DXJSON.safeTreeToValue(apiCallOnObject("describe", RetryStrategy.SAFE_TO_RETRY),
                Describe.class);
    }

    /**
     * Destroys the project and all its contents.
     */
    public void destroy() {
        this.apiCallOnObject("destroy", RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Destroys the project and all its contents, optionally terminating all running jobs.
     *
     * @param terminateJobs if true, terminates any running jobs in the project
     */
    public void destroy(boolean terminateJobs) {
        this.apiCallOnObject("destroy",
                MAPPER.valueToTree(new ProjectTerminateRequest(terminateJobs)),
                RetryStrategy.SAFE_TO_RETRY);
    }

    // The following unimplemented methods are sorted in approximately
    // decreasing order of usefulness to Java clients.

    // TODO: /project-xxxx/describe
    // TODO: /project-xxxx/addTags
    // TODO: /project-xxxx/removeTags
    // TODO: /project-xxxx/setProperties
    // TODO: /project-xxxx/update

    // TODO: /project-xxxx/invite
    // TODO: /project-xxxx/decreasePermissions
    // TODO: /project-xxxx/leave

    // TODO: /project-xxxx/transfer
    // TODO: /project-xxxx/acceptTransfer
}
