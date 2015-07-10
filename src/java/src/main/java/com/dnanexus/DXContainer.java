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
        Preconditions.checkArgument(projectOrContainerId.startsWith("project-")
                || projectOrContainerId.startsWith("container-"), "Container ID "
                + projectOrContainerId + " must start with project- or container-");
    }

    DXContainer(String containerId) {
        // Perform no prefix check in super constructor because we'll be doing our own here
        super(containerId, null, null);
        checkContainerId(containerId);
    }

    DXContainer(String containerId, DXEnvironment env) {
        // Perform no prefix check in super constructor because we'll be doing our own here
        super(containerId, null, env);
        checkContainerId(containerId);
    }

    /**
     * Returns a {@code DXProject} or {@code DXContainer} associated with an existing project or
     * container.
     *
     * @param projectOrContainerId String starting with {@code "container-"} or {@code "project-"}
     *
     * @return {@code DXProject} or {@code DXContainer}
     *
     * @throws NullPointerException if {@code projectOrContainerId} is null
     */
    public static DXContainer getInstance(String projectOrContainerId) {
        if (projectOrContainerId.startsWith("project-")) {
            return DXProject.getInstance(projectOrContainerId);
        }
        return new DXContainer(projectOrContainerId);
    }

    /**
     * Returns a {@code DXProject} or {@code DXContainer} associated with an existing project or
     * container, using the specified environment.
     *
     * @param projectOrContainerId String starting with {@code "container-"} or {@code "project-"}
     * @param env environment to use to make subsequent API requests
     *
     * @return {@code DXProject} or {@code DXContainer}
     *
     * @throws NullPointerException if {@code projectOrContainerId} or {@code env} is null
     */
    public static DXContainer getInstanceWithEnvironment(String projectOrContainerId,
            DXEnvironment env) {
        Preconditions.checkNotNull(env, "env may not be null");
        if (projectOrContainerId.startsWith("project-")) {
            return DXProject.getInstanceWithEnvironment(projectOrContainerId, env);
        }
        return new DXContainer(projectOrContainerId, env);
    }

    /**
     * A request to the /container-xxxx/move route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class ContainerMoveRequest {
        @JsonProperty
        private final List<String> objects;
        @JsonProperty
        private final List<String> folders;
        @JsonProperty
        private final String destination;

        private ContainerMoveRequest(List<String> objects, List<String> folders, String destination) {
            this.objects = ImmutableList.copyOf(objects);
            this.folders = ImmutableList.copyOf(folders);
            this.destination = destination;
        }
    }

    /**
     * A response from the /container-xxxx/move route.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ContainerMoveResponse {}

    /**
     * Moves the specified data objects and folders to a destination folder in the same container.
     *
     * @param objects data objects to be moved
     * @param folders full paths to the folders to be moved (Strings starting with {@code "/"})
     * @param destinationFolder full path to the destination folder (a String starting with
     *        {@code "/"})
     */
    public void move(List<? extends DXDataObject> objects, List<String> folders,
            String destinationFolder) {
        ImmutableList.Builder<String> objectIds = ImmutableList.builder();
        for (DXDataObject dataObj : objects) {
            objectIds.add(dataObj.getId());
        }
        DXAPI.containerMove(this.getId(), new ContainerMoveRequest(objectIds.build(), folders,
                destinationFolder), ContainerMoveResponse.class);
    }

    /**
     * Moves the specified folders to a destination folder in the same container.
     *
     * @param folders full paths to the folders to be moved (Strings starting with {@code "/"})
     * @param destinationFolder full path to the destination folder (a String starting with
     *        {@code "/"})
     */
    public void moveFolders(List<String> folders, String destinationFolder) {
        move(ImmutableList.<DXDataObject>of(), folders, destinationFolder);
    }

    /**
     * Moves the specified data objects to a destination folder in the same container.
     *
     * @param objects data objects to be moved
     * @param destinationFolder full path to the destination folder (A String starting with
     *        {@code "/"})
     */
    public void moveObjects(List<? extends DXDataObject> objects, String destinationFolder) {
        move(objects, ImmutableList.<String>of(), destinationFolder);
    }

    /**
     * A request to the /container-xxxx/newFolder route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class ContainerNewFolderRequest {
        @JsonProperty
        private final String folder;
        @JsonProperty
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
     * A response from the /container-xxxx/newFolder route.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ContainerNewFolderResponse {}

    /**
     * Creates the specified folder.
     *
     * @param folderPath full path to the folder to be created (a String starting with {@code "/"})
     */
    public void newFolder(String folderPath) {
        DXAPI.containerNewFolder(this.getId(), new ContainerNewFolderRequest(folderPath),
                ContainerNewFolderResponse.class);
    }

    /**
     * Creates the specified folder, optionally creating parent folders as well.
     *
     * @param folderPath full path to the folder to be created (a String starting with {@code "/"})
     * @param parents if true, create all parent folders of the requested path
     */
    public void newFolder(String folderPath, boolean parents) {
        DXAPI.containerNewFolder(this.getId(), new ContainerNewFolderRequest(folderPath, parents),
                ContainerNewFolderResponse.class);
    }

    /**
     * A request to the /container-xxxx/renameFolder route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class ContainerRenameFolderRequest {
        @JsonProperty
        private final String folder;
        @JsonProperty
        private final String name;

        private ContainerRenameFolderRequest(String folder, String name) {
            this.folder = folder;
            this.name = name;
        }
    }

    /**
     * A response from the /container-xxxx/renameFolder route.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ContainerRenameFolderResponse {}

    /**
     * Renames the specified folder.
     *
     * <p>
     * This method renames a folder in the same parent folder (i.e. only changes its basename).
     * </p>
     *
     * @param folderPath full path to the folder being renamed (a String starting with {@code "/"})
     * @param name new basename for the folder
     */
    public void renameFolder(String folderPath, String name) {
        // TODO: document "move" method for moving to other folders, when it's
        // available
        DXAPI.containerRenameFolder(this.getId(),
                new ContainerRenameFolderRequest(folderPath, name),
                ContainerRenameFolderResponse.class);
    }

    /**
     * A request to the /container-xxxx/removeFolder route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class ContainerRemoveFolderRequest {
        @JsonProperty
        private final String folder;
        @JsonProperty
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
     * A response from the /container-xxxx/removeFolder route.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ContainerRemoveFolderResponse {}

    /**
     * Removes the specified folder.
     *
     * @param folderPath path to the folder to be removed (String starting with {@code "/"})
     */
    public void removeFolder(String folderPath) {
        DXAPI.containerRemoveFolder(this.getId(), new ContainerRemoveFolderRequest(folderPath),
                ContainerRemoveFolderResponse.class);
    }

    /**
     * Removes the specified folder, optionally removing all subfolders as well.
     *
     * @param folderPath full path to the folder to be removed (a String starting with {@code "/"})
     * @param recurse if true, deletes all objects and subfolders in the folder as well
     */
    public void removeFolder(String folderPath, boolean recurse) {
        DXAPI.containerRemoveFolder(this.getId(), new ContainerRemoveFolderRequest(folderPath,
                recurse), ContainerRemoveFolderResponse.class);
    }

    /**
     * A request to the /container-xxxx/removeObjects route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class ContainerRemoveObjectsRequest {
        @JsonProperty
        private final List<String> objects;

        private ContainerRemoveObjectsRequest(List<? extends DXDataObject> objects) {
            ImmutableList.Builder<String> objectIds = ImmutableList.builder();
            for (DXDataObject object : objects) {
                objectIds.add(object.getId());
            }
            this.objects = objectIds.build();
        }
    }

    /**
     * A response from the /container-xxxx/removeObjects route.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ContainerRemoveObjectsResponse {}

    /**
     * Removes the specified objects from the container.
     *
     * <p>
     * Removal propagates to hidden linked objects in the same container that are no longer
     * reachable from any visible object.
     * </p>
     *
     * @param objects List of objects to be removed
     */
    public void removeObjects(List<? extends DXDataObject> objects) {
        DXAPI.containerRemoveObjects(this.getId(), new ContainerRemoveObjectsRequest(objects),
                ContainerRemoveObjectsResponse.class);
    }

    @JsonInclude(Include.NON_NULL)
    private static class ContainerListFolderRequest {
        @JsonProperty
        private final String folder;

        private ContainerListFolderRequest(String folder) {
            this.folder = folder;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ContainerListFolderResponse {
        @JsonIgnoreProperties(ignoreUnknown = true)
        private static class ObjectHash {
            @JsonProperty
            private String id;
        }

        @JsonProperty
        private List<String> folders;
        @JsonProperty
        private List<ObjectHash> objects;
    }

    /**
     * Represents the contents of a folder.
     *
     * <p>
     * The contents are as of the time this object was created.
     * </p>
     */
    public static class FolderContents {

        private final List<DXDataObject> dataObjects;
        private final List<String> subfolders;

        private FolderContents(Collection<ContainerListFolderResponse.ObjectHash> dataObjectIds,
                Collection<String> subfolders, DXContainer container, DXEnvironment env) {
            ImmutableList.Builder<DXDataObject> dataObjects = ImmutableList.builder();
            for (ContainerListFolderResponse.ObjectHash object : dataObjectIds) {
                dataObjects.add(DXDataObject.getInstanceWithEnvironment(object.id, container, env));
            }
            this.dataObjects = dataObjects.build();
            this.subfolders = ImmutableList.copyOf(subfolders);
        }

        /**
         * Lists all subfolders of the specified folder.
         *
         * @return List containing the full path of each subfolder (Strings starting with
         *         {@code "/"})
         */
        public List<String> getSubfolders() {
            return this.subfolders;
        }

        /**
         * Lists all data objects in the specified folder.
         *
         * @return List containing a {@code DXDataObject} for each data object
         */
        public List<DXDataObject> getObjects() {
            return this.dataObjects;
        }

    }

    /**
     * Lists the data objects and subfolders inside the specified folder.
     *
     * @param folderPath full path to a folder (a String starting with {@code "/"})
     *
     * @return a {@code FolderContents} giving the contents of the specified folder
     */
    public FolderContents listFolder(String folderPath) {
        // TODO: provide variants to obtain only subfolders or only objects
        // without retrieving both

        // TODO: parameters describe and includeHidden
        ContainerListFolderResponse r =
                DXAPI.containerListFolder(this.getId(), new ContainerListFolderRequest(folderPath),
                        ContainerListFolderResponse.class);
        return new FolderContents(r.objects, r.folders, this, this.env);
    }

    // TODO: /container-xxxx/clone

}
