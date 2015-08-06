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

/** \file
 *
 * \brief Containers and projects.
 *
 * Data containers (DXContainer) and projects (DXProject, a subclass of %DXContainer).
 */

#ifndef DXCPP_BINDINGS_DXPROJECT_H
#define DXCPP_BINDINGS_DXPROJECT_H

#include "../bindings.h"

namespace dx {
  //! A generic container for data objects.

  ///
  /// Every data object on the DNAnexus Platform must reside in a data container. When an object is
  /// first created, it may only reside in a single container. However, objects may be cloned into
  /// other containers once the objects have been closed (and their contents may no longer be
  /// modified). See <a
  /// href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Data-Object-Lifecycle">Data Object
  /// Lifecycle</a> in the API specification for more information.
  ///
  /// Projects (DXProject) are containers that provide additional functionality for collaboration
  /// between users.
  ///

  class DXContainer {
   private:
    std::string dxid_;

   public:

    /**
     * Creates a %DXContainer handler for the specified data container ID.
     *
     * @param dxid A string containing a data container ID.
     */
    DXContainer(const std::string &dxid=config::CURRENT_PROJECT()) : dxid_(dxid) { }

    /**
     * @param dxid Associates the handler with the given data container ID.
     *
     * @note No error checking is done in this function.
     */
    void setID(const std::string &dxid) { dxid_ = dxid; }

    /**
     * @return ID of the associated data container.
     */
    std::string getID() const { return dxid_; }

    /**
     * This default conversion to string returns the container ID so a handler can always be passed
     * in place of a string argument that expects a container or project ID.
     *
     * @return ID of the associated data container.
     */
    operator std::string() { return dxid_;}

    /**
     * Returns a description of the associated container as given by the /container-xxxx/describe API
     * method.
     *
     * @param folders If true, a complete list of the folders in the data container is included in
     * the output.
     *
     * @return JSON hash containing the output of the describe call.
     */
    // TODO: add wiki link for /class-xxx/describe
    dx::JSON describe(bool folders=false) const;

    /**
     * Moves the specified objects and/or folders in the associated data container to the specified
     * folder.
     *
     * See the <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Folders%20and%20Deletion#API-method%3A-%2Fclass-xxxx%2Fmove">/class-xxxx/move</a> API method for more info.
     *
     * @param objects A JSON array of strings containing the object ID(s) to be moved.
     * @param folders A JSON array of strings containing the folder route(s) to be moved.
     * @param dest_folder The full path of the destination folder.
     */
    void move(const dx::JSON &objects,
              const dx::JSON &folders,
              const std::string &dest_folder) const;

    /**
     * Clones the specified objects and/or folders from the associated data container to another data
     * container.
     *
     * See the <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Cloning#API-method%3A-%2Fclass-xxxx%2Fclone">/class-xxx/clone</a> API method for more info.
     *
     * @param objects A JSON array of strings containing the object ID(s) to be cloned.
     * @param folders A JSON array of strings containing the folder route(s) to be cloned.
     * @param dest_container ID of the container into which the selected objects should be cloned.
     * @param dest_folder The full path of the destination folder in the destination container.
     */
    void clone(const dx::JSON &objects,
               const dx::JSON &folders,
               const std::string &dest_container,
               const std::string &dest_folder="/") const;

    // Folder specific

    /**
     * Creates a new folder in the associated data container.
     *
     * @param folder The full path of the new folder to be created.
     * @param parents Whether to create the parent folders if they do not already exist.
     */
    void newFolder(const std::string &folder, bool parents=false) const;

    /**
     * Lists the contents of a folder in the associated data container.
     *
     * @param folder A string representing the full path to the folder to be listed.
     *
     * @return A JSON hash of the form: <code>{"objects": [{"id": id-1}, {"id": id-2}, ...], "folders": ["full-path-to-folder-1", "full-path-to-folder-2", ...]}</code>
     */
    dx::JSON listFolder(const std::string &folder="/") const;

    /**
     * Moves a folder in the associated data container (and all the objects and subfolders it
     * contains) to the specified destination folder.
     *
     * See the <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Folders%20and%20Deletion#API-method%3A-%2Fclass-xxxx%2Fmove">/class-xxxx/move</a> API method for more info.
     *
     * @param folder The full path of the folder to be moved.
     * @param dest_folder The full path of the destination folder.
     */
    void moveFolder(const std::string &folder,
                    const std::string &dest_folder) const;

    /**
     * Removes a given folder and all its data content.
     *
     * The folder must be empty unless <code>recurse</code> is set to <code>true</code>.
     *
     * @param folder The full path of the folder to be removed.
     * @param recurse Boolean indicating whether to recursively remove all objects and folders contained in the target.
     */
    void removeFolder(const std::string &folder, const bool recurse=false) const;

    // Objects-specific

    /**
     * Move objects in the associated data container to the specified destination folder.
     *
     * See the <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Folders%20and%20Deletion#API-method%3A-%2Fclass-xxxx%2Fmove">/class-xxxx/move</a> API method for more info.
     *
     * @param objects A JSON array of strings containing the object ID(s) to be moved.
     * @param dest_folder The full path of the destination folder.
     */
    void moveObjects(const dx::JSON &objects,
                     const std::string &dest_folder) const {
      move(objects, dx::JSON(dx::JSON_ARRAY), dest_folder);
    }

    /**
     * Removes the selected object(s) from the data container.
     *
     * @param objects A JSON array of strings containing the object ID(s) to be removed.
     */
    void removeObjects(const dx::JSON &objects) const;

    /**
     * Clones the specified folder to another data container. The of the folder is preserved in the
     * destination data container.
     *
     * Any hidden objects contained in a folder to be cloned are only cloned if a visible ancestor is
     * also cloned.
     *
     * See the <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Cloning#API-method%3A-%2Fclass-xxxx%2Fclone">/class-xxx/clone</a> API method for more info.
     *
     * @param folder The full path of the folder to be cloned.
     * @param dest_container ID of the container into which the folder should be cloned.
     * @param dest_folder The full path of the destination folder in the destination container.
     */
    void cloneFolder(const std::string &folder,
                     const std::string &dest_container,
                     const std::string &dest_folder) const {
      clone(dx::JSON(dx::JSON_ARRAY), dx::JSON::parse("[\"" + folder + "\"]"), dest_container, dest_folder);
    }

    /**
     * Clones the specified object(s) from the associated data container to another data container.
     *
     * See the <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Cloning#API-method%3A-%2Fclass-xxxx%2Fclone">/class-xxx/clone</a> API method for more info.
     *
     * @param objects A JSON array of strings containing the object ID(s) to be cloned.
     * @param dest_container ID of the container into which the selected objects should be cloned.
     * @param dest_folder The full path of the destination folder in the destination container.
     */
    void cloneObjects(const dx::JSON &objects,
                      const std::string &dest_container,
                      const std::string &dest_folder) const {
      clone(objects, dx::JSON(dx::JSON_ARRAY), dest_container, dest_folder);
    }
  };

  //! A DXContainer with additional functionality for collaboration.

  ///
  /// In most day-to-day operations on the DNAnexus Platform, users will interact directly with
  /// projects rather than generic containers.
  ///

  class DXProject : public DXContainer {
   public:

    /**
     * Creates a %DXProject handler for the specified remote project.
     *
     * @param dxid Project ID.
     */
    DXProject(const std::string &dxid=config::CURRENT_PROJECT()) : DXContainer(dxid) { }

    /**
     * Updates the remote project with the provided options, as specified in the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Projects#API-method%3A-%2Fproject-xxxx%2Fupdate">/project-xxxx/update</a>
     * method.
     *
     * @param to_update JSON hash to be provided to <code>/project-xxxx/update</code>.
     */
    void update(const dx::JSON &to_update) const;

    /**
     * Destroys the remote project. All objects in the project are removed. Any jobs running in the
     * project context are terminated.
     */
    void destroy() const;

    /**
     * Invites another person (or PUBLIC) to the remote project. If the invitee is another person,
     * they will receive the specified permission when they accept the invitation.
     *
     * See the <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Project-Permissions-and-Sharing#API-method%3A-%2Fproject-xxxx%2Finvite">/project-xxxx/invite</a> API method for more info.
     *
     * @param invitee Username (of the form "user-USERNAME") or email of the
     * person to be invited to the project. Use "PUBLIC" to make the project
     * publicly available (in which case level must be set to "VIEW").
     * @param level Permission level that the invitee would get ("VIEW", "CONTRIBUTE", "ADMINISTER").
     */
    void invite(const std::string &invitee, const std::string &level) const;

    /**
     * Decreases the permissions of the specified user in the remote project.
     *
     * See the <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Project-Permissions-and-Sharing#API-method%3A-%2Fproject-xxxx%2FdecreasePermissions">/project-xxxx/decreasePermissions</a> API method for more info.
     *
     * @param member Username (of the form "user-USERNAME") of the project member
     * whose permissions will be decreased.
     * @param level The new permission level for the user ("VIEW", "CONTRIBUTE", "ADMINISTER").
     */
    // TODO: link to wiki docs
    void decreasePerms(const std::string &member, const std::string &level) const;
  };
}
#endif
