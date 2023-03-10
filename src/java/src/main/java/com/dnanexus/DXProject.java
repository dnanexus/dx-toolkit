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

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

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

    public ArchiveRequestBuilder archive() {
        return new ArchiveRequestBuilder();
    }

    public UnarchiveRequestBuilder unarchive() {
        return new UnarchiveRequestBuilder();
    }

    @JsonInclude(Include.NON_NULL)
    private static class ArchiveRequest {
        @JsonProperty
        private final List<String> files;
        @JsonProperty
        private final String folder;
        @JsonProperty
        private final Boolean recurse;
        @JsonProperty
        private final Boolean allCopies;

        public ArchiveRequest(List<String> files, String folder, Boolean recurse, Boolean allCopies) {
            this.files = files.isEmpty() ? null : files;
            this.folder = folder;
            this.recurse = recurse;
            this.allCopies = allCopies;
        }

    }

    public class ArchiveRequestBuilder {
        private static final String FILE_NULL_ERR = "file may not be null";
        private static final String FILES_LIST_NULL_ERR = "files collection may not be null";
        private static final String FILES_FOLDER_NONEXCLUSIVE_ERR = "Files and folder params are mutually exclusive";
        private final List<String> files = Lists.newArrayList();
        private String folder;
        private Boolean recurse;
        private Boolean allCopies;

        public ArchiveRequestBuilder addFile(DXFile file) {
            return addFile(Preconditions.checkNotNull(file, FILE_NULL_ERR).getId());
        }

        public ArchiveRequestBuilder addFiles(DXFile... files) {
            Preconditions.checkNotNull(files, FILES_LIST_NULL_ERR);
            return addFiles(Lists.newArrayList(files));
        }
        public ArchiveRequestBuilder addFiles(List<DXFile> files) {
            Preconditions.checkNotNull(files, FILES_LIST_NULL_ERR);
            for (DXFile file : files) {
                addFile(file);
            }
            return this;
        }

        public ArchiveRequestBuilder addFile(String file) {
            Preconditions.checkState(this.folder == null, FILES_FOLDER_NONEXCLUSIVE_ERR);
            files.add(Preconditions.checkNotNull(file, FILE_NULL_ERR));
            return this;
        }

        public ArchiveRequestBuilder addFiles(String... files) {
            Preconditions.checkNotNull(files, FILES_LIST_NULL_ERR);
            return addFiles(Lists.newArrayList(files));
        }

        public ArchiveRequestBuilder addFiles(Collection<String> files) {
            Preconditions.checkNotNull(files, FILES_LIST_NULL_ERR);
            for (String file : files) {
                addFile(file);
            }
            return this;
        }

        public ArchiveRequestBuilder setFolder(String folder) {
            return setFolder(folder, null);
        }

        public ArchiveRequestBuilder setFolder(String folder, Boolean recurse) {
            Preconditions.checkState(this.files.isEmpty(), FILES_FOLDER_NONEXCLUSIVE_ERR);
            Preconditions.checkState(this.folder == null, "Cannot call setFolder more than once");
            this.folder = Preconditions.checkNotNull(folder, "folder may not be null");
            this.recurse = recurse;
            return this;
        }

        public ArchiveRequestBuilder setAllCopies(Boolean allCopies) {
            Preconditions.checkState(this.allCopies == null,
                    "Cannot call setAllCopies more than once");
            this.allCopies = allCopies;
            return this;
        }

        public ArchiveResults execute() {
            return DXJSON.safeTreeToValue(apiCallOnObject("archive",
                    MAPPER.valueToTree(new ArchiveRequest(files, folder, recurse, allCopies)),
                    RetryStrategy.SAFE_TO_RETRY), ArchiveResults.class);
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ArchiveResults {
        @JsonProperty
        private int count;

        public int getCount() {
            return count;
        }

    }

    @JsonInclude(Include.NON_NULL)
    private static class UnarchiveRequest {
        @JsonProperty
        private final List<String> files;
        @JsonProperty
        private final String folder;
        @JsonProperty
        private final Boolean recurse;
        @JsonProperty
        private final UnarchivingRate rate;
        @JsonProperty
        private final Boolean dryRun;

        public UnarchiveRequest(List<String> files, String folder, Boolean recurse, UnarchivingRate rate, Boolean dryRun) {
            this.files = files.isEmpty() ? null : files;
            this.folder = folder;
            this.recurse = recurse;
            this.rate = rate;
            this.dryRun = dryRun;
        }

    }

    public class UnarchiveRequestBuilder {
        private static final String FILE_NULL_ERR = "file may not be null";
        private static final String FILES_LIST_NULL_ERR = "files collection may not be null";
        private static final String FILES_FOLDER_NONEXCLUSIVE_ERR = "Files and folder params are mutually exclusive";
        private final List<String> files = Lists.newArrayList();
        private String folder;
        private Boolean recurse;
        private UnarchivingRate rate;
        private Boolean dryRun;

        public UnarchiveRequestBuilder addFile(DXFile file) {
            return addFile(Preconditions.checkNotNull(file, FILE_NULL_ERR).getId());
        }

        public UnarchiveRequestBuilder addFiles(DXFile... files) {
            Preconditions.checkNotNull(files, FILES_LIST_NULL_ERR);
            return addFiles(Lists.newArrayList(files));
        }
        public UnarchiveRequestBuilder addFiles(List<DXFile> files) {
            Preconditions.checkNotNull(files, FILES_LIST_NULL_ERR);
            for (DXFile file : files) {
                addFile(file);
            }
            return this;
        }

        public UnarchiveRequestBuilder addFile(String file) {
            Preconditions.checkState(this.folder == null, FILES_FOLDER_NONEXCLUSIVE_ERR);
            files.add(Preconditions.checkNotNull(file, FILE_NULL_ERR));
            return this;
        }

        public UnarchiveRequestBuilder addFiles(String... files) {
            Preconditions.checkNotNull(files, FILES_LIST_NULL_ERR);
            return addFiles(Lists.newArrayList(files));
        }

        public UnarchiveRequestBuilder addFiles(Collection<String> files) {
            Preconditions.checkNotNull(files, FILES_LIST_NULL_ERR);
            for (String file : files) {
                addFile(file);
            }
            return this;
        }

        public UnarchiveRequestBuilder setFolder(String folder) {
            return setFolder(folder, null);
        }

        public UnarchiveRequestBuilder setFolder(String folder, Boolean recurse) {
            Preconditions.checkState(this.files.isEmpty(), FILES_FOLDER_NONEXCLUSIVE_ERR);
            Preconditions.checkState(this.folder == null, "Cannot call setFolder more than once");
            this.folder = Preconditions.checkNotNull(folder, "folder may not be null");
            this.recurse = recurse;
            return this;
        }

        public UnarchiveRequestBuilder setRate(UnarchivingRate rate) {
            Preconditions.checkState(this.rate == null,
                    "Cannot call setRate more than once");
            this.rate = rate;
            return this;
        }

        public UnarchiveRequestBuilder setDryRun(Boolean dryRun) {
            Preconditions.checkState(this.dryRun == null,
                    "Cannot call setDryRun more than once");
            this.dryRun = dryRun;
            return this;
        }

        public UnarchiveResult execute() {
            return DXJSON.safeTreeToValue(apiCallOnObject("unarchive",
                    MAPPER.valueToTree(new UnarchiveRequest(files, folder, recurse, rate, dryRun)),
                    RetryStrategy.SAFE_TO_RETRY), UnarchiveResult.class);
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UnarchiveResult {
        @JsonProperty
        private int files;
        @JsonProperty
        private int size;
        @JsonProperty
        private float cost;

        protected UnarchiveResult() {
        }

        public int getFiles() {
            return files;
        }

        public int getSize() {
            return size;
        }

        public float getCost() {
            return cost;
        }

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
