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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A project (a container providing features for data sharing and
 * collaboration).
 */
public class DXProject extends DXContainer {

    private static void checkProjectId(String projectId) {
        Preconditions.checkArgument(projectId.startsWith("project-"), "Project ID " + projectId
                + " must start with project-");
    }

    /**
     * Contains metadata for a project. All accessors reflect the state of the
     * project at the time that this object was created.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Describe {
        @SuppressWarnings("unused")
        @JsonProperty
        private String id;
        @JsonProperty
        private String name;

        /**
         * Creates a {@code Describe} object with all empty metadata.
         */
        private Describe() {
        }

        // TODO: other project metadata fields

        /**
         * Returns the name of the project.
         */
        public String getName() {
            return this.name;
        }

    }

    /**
     * Builder class for creating a new {@code DXProject} object. To obtain an
     * instance, call {@link DXProject#newProject()}.
     */
    public static class Builder {

        private String name = null;

        private Builder() {
        }

        /**
         * Sets the name of the new project.
         */
        public Builder setName(String name) {
            Preconditions.checkNotNull(name, "Name must be specified");
            Preconditions.checkArgument(this.name == null, "Name may not be specified more than once");
            Preconditions.checkArgument(name.length() > 0, "Name must be non-empty");
            this.name = name;
            return this;
        }

        @VisibleForTesting
        JsonNode buildRequestHash() {
            // Use this method to test the JSON hash created by a particular
            // builder call without actually executing the request.
            Preconditions.checkArgument(this.name != null, "name must be specified");
            return MAPPER.valueToTree(new ProjectNewRequest(this));
        }

        /**
         * Creates the project and returns a {@code DXProject} corresponding to
         * the newly created project.
         */
        public DXProject build() {
            return new DXProject(DXJSON.safeTreeToValue(
                    (DXAPI.projectNew(this.buildRequestHash())), ProjectNewResponse.class).id);
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class ProjectNewRequest {
        @SuppressWarnings("unused")
        @JsonProperty
        private String name;

        private ProjectNewRequest(Builder builder) {
            this.name = builder.name;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ProjectNewResponse {
        @JsonProperty
        private String id;
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
     * @throws NullPointerException
     *             If {@code projectId} is null
     */
    public static DXProject getInstance(String projectId) {
        return new DXProject(projectId);
    }

    /**
     * Returns a {@code DXProject} associated with an existing project using the
     * specified environment.
     *
     * @throws NullPointerException
     *             If {@code projectId} or {@code env} is null
     */
    public static DXProject getInstanceWithEnvironment(String projectId, DXEnvironment env) {
        Preconditions.checkNotNull(env);
        return new DXProject(projectId, env);
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
     */
    public static Builder newProject() {
        return new Builder();
    }

    /**
     * Returns metadata about the project.
     */
    public Describe describe() {
        return DXJSON.safeTreeToValue(apiCallOnObject("describe"), Describe.class);
    }

    /**
     * Destroys the project and all its contents.
     */
    public void destroy() {
        // TODO: supply terminateJobs option
        this.apiCallOnObject("destroy");
    }

    // TODO: /project-xxxx/describe
    // TODO: /project-xxxx/addTags
    // TODO: /project-xxxx/removeTags
    // TODO: /project-xxxx/setProperties
    // TODO: /project-xxxx/update
}
