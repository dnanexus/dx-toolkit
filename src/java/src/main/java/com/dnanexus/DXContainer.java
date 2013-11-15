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

import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * A container (a logical collection for storage and organization of data).
 */
public class DXContainer extends DXObject {

    private static void checkContainerId(String projectOrContainerId) {
        Preconditions.checkArgument(
                projectOrContainerId.startsWith("project-") || projectOrContainerId.startsWith("container-"),
                "Container ID " + projectOrContainerId + " must start with project- or container-");
    }

    DXContainer(String containerId) {
        super(containerId, null);
        checkContainerId(containerId);
    }

    DXContainer(String containerId, DXEnvironment env) {
        super(containerId, env);
        checkContainerId(containerId);
    }

    /**
     * Returns a {@code DXContainer} or {@code DXProject} associated with an
     * existing container or project.
     *
     * @param projectOrContainerId
     *            String starting with {@code "container-"} or
     *            {@code "project-"}
     *
     * @return {@code DXContainer} or {@code DXProject}
     *
     * @throws NullPointerException
     *             If {@code projectOrContainerId} is null
     */
    public static DXContainer getInstance(String projectOrContainerId) {
        if (projectOrContainerId.startsWith("project-")) {
            return DXProject.getInstance(projectOrContainerId);
        }
        return new DXContainer(projectOrContainerId);
    }

    /**
     * Returns a {@code DXContainer} or {@code DXProject} associated with an
     * existing container or project, using the specified environment.
     *
     * @param projectOrContainerId
     *            String starting with {@code "container-"} or
     *            {@code "project-"}
     * @param env
     *            Environment
     *
     * @return {@code DXContainer} or {@code DXProject}
     *
     * @throws NullPointerException
     *             If {@code projectOrContainerId} or {@code env} is null
     */
    public static DXContainer getInstanceWithEnvironment(String projectOrContainerId, DXEnvironment env) {
        Preconditions.checkNotNull(env);
        if (projectOrContainerId.startsWith("project-")) {
            return DXProject.getInstanceWithEnvironment(projectOrContainerId, env);
        }
        return new DXContainer(projectOrContainerId, env);
    }

    /**
     * A request to the /container-xxxx/newFolder route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class ContainerNewFolderRequest {
        @SuppressWarnings("unused")
        @JsonProperty
        private final String folder;
        @JsonProperty
        @SuppressWarnings("unused")
        private final Boolean parents;

        private ContainerNewFolderRequest(String folder) {
            this.folder = folder;
            this.parents = null;
        }

        private ContainerNewFolderRequest(String folder, boolean parents) {
            this.folder = folder;
            this.parents = parents;
        }
    }

    /**
     * Creates the specified folder.
     *
     * @param folderPath
     *            A string beginning with {@code "/"}
     */
    public void newFolder(String folderPath) {
        DXAPI.containerNewFolder(this.getId(), MAPPER.valueToTree(new ContainerNewFolderRequest(folderPath)));
    }

    /**
     * Creates the specified folder, optionally creating parent folders as
     * well.
     *
     * @param folderPath
     *            A string beginning with {@code "/"}
     * @param parents
     *            If true, create all parent folders of the requested path
     */
    public void newFolder(String folderPath, boolean parents) {
        DXAPI.containerNewFolder(this.getId(), MAPPER.valueToTree(new ContainerNewFolderRequest(folderPath, parents)));
    }

    /**
     * A request to the /container-xxxx/renameFolder route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class ContainerRenameFolderRequest {
        @SuppressWarnings("unused")
        @JsonProperty
        private final String folder;
        @JsonProperty
        @SuppressWarnings("unused")
        private final String name;

        private ContainerRenameFolderRequest(String folder, String name) {
            this.folder = folder;
            this.name = name;
        }
    }

    /**
     * Renames the specified folder.
     *
     * <p>
     * This method renames a folder in the same parent folder (i.e. only changes
     * its basename).
     * </p>
     *
     * @param folderPath
     *            Path to the folder being renamed (String starting with
     *            {@code "/"})
     * @param name
     *            New basename for the folder
     */
    public void renameFolder(String folderPath, String name) {
        // TODO: document "move" method for moving to other folders, when it's
        // available
        DXAPI.containerRenameFolder(this.getId(),
                MAPPER.valueToTree(new ContainerRenameFolderRequest(folderPath, name)));
    }

    /**
     * A request to the /container-xxxx/renameFolder route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class ContainerRemoveFolderRequest {
        @SuppressWarnings("unused")
        @JsonProperty
        private final String folder;
        @JsonProperty
        @SuppressWarnings("unused")
        private final Boolean recurse;

        private ContainerRemoveFolderRequest(String folder) {
            this.folder = folder;
            this.recurse = null;
        }

        private ContainerRemoveFolderRequest(String folder, boolean recurse) {
            this.folder = folder;
            this.recurse = recurse;
        }
    }

    /**
     * Removes the specified folder.
     *
     * @param folderPath
     *            Path to the folder to be removed (String starting with
     *            {@code "/"})
     */
    public void removeFolder(String folderPath) {
        DXAPI.containerRemoveFolder(this.getId(), MAPPER.valueToTree(new ContainerRemoveFolderRequest(folderPath)));
    }

    /**
     * Removes the specified folder, optionally removing all subfolders as
     * well.
     *
     * @param folderPath
     *            Path to the folder to be removed (String starting with
     *            {@code "/"})
     * @param recurse
     *            If true, deletes all objects and subfolders in the folder as
     *            well
     */
    public void removeFolder(String folderPath, boolean recurse) {
        DXAPI.containerRemoveFolder(this.getId(),
                MAPPER.valueToTree(new ContainerRemoveFolderRequest(folderPath, recurse)));
    }

    @JsonInclude(Include.NON_NULL)
    private static class ContainerListFolderRequest {
        @SuppressWarnings("unused")
        @JsonProperty
        private final String folder;

        private ContainerListFolderRequest(String folder) {
            this.folder = folder;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ContainerListFolderResponse {
        // TODO: returning objects is not implemented yet, only folders!
        @JsonIgnoreProperties(ignoreUnknown = true)
        private static class ObjectHash {
            @SuppressWarnings("unused")
            @JsonProperty
            private String id;
        }

        @JsonProperty
        private List<String> folders;
        @SuppressWarnings("unused")
        @JsonProperty
        private List<ObjectHash> objects;
    }

    /**
     * Represents the contents of a folder.
     *
     * <p>The contents are as of the time this object was created.</p>
     */
    public static class FolderContents {

        private final List<String> subfolders;

        // TODO: nail down DXDataObjects interfaces and then return objects too
        private FolderContents(Collection<String> subfolders) {
            this.subfolders = ImmutableList.copyOf(subfolders);
        }

        /**
         * Returns the full paths to all subfolders of the specified folder.
         *
         * @return List containing one String for each subfolder
         */
        public List<String> getSubfolders() {
            return this.subfolders;
        }

    }

    /**
     * Returns the contents of the specified folder.
     *
     * @param folderPath
     *            Folder to list the contents of (String starting with
     *            {@code "/"})
     */
    public FolderContents listFolder(String folderPath) {
        // TODO: parameters describe, only, and includeHidden
        ContainerListFolderResponse r =
                DXJSON.safeTreeToValue(
                        DXAPI.containerListFolder(this.getId(),
                                MAPPER.valueToTree(new ContainerListFolderRequest(folderPath))),
                        ContainerListFolderResponse.class);
        return new FolderContents(r.folders);
    }

    // The following are probably only useful when we have DXDataObjects more
    // fleshed out

    // TODO: /container-xxxx/move
    // TODO: /container-xxxx/removeObjects
    // TODO: /container-xxxx/clone

}
