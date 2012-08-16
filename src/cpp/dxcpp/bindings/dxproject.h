#ifndef DXCPP_BINDINGS_DXPROJECT_H
#define DXCPP_BINDINGS_DXPROJECT_H

#include "../bindings.h"

class DXContainer {
 private:
  std::string dxid_;
 public:
 DXContainer(const std::string &dxid=g_WORKSPACE_ID) : dxid_(dxid) { }

  /** 
   * @param dxid Associate the handler to given data container id.
   * @note No error checking is done in this function
   */
  void setID(const std::string &dxid) { dxid_ = dxid; }
  
  /**
   * @return ID of the associated data container
   */
  std::string getID() const { return dxid_; }
  
  /**
   * Default conversion to string is to its project ID so a handler
   * can always be passed in place of a string argument that expects
   * a project ID.
   */
  operator std::string() { return dxid_;}

  /**
   * @param folders If true, return list of folders in the data container
   * @return The output of describe call (JSON hash) on the data container
   */
  dx::JSON describe(bool folders=false) const;

  /**
   * Move specified objects and/or folders to other folder
   * in the same data container.
   *
   * @param objects a JSON array of object IDs to be moved
   * Empty array denotes that no object is to be moved.
   * @param folders An array of strings of the folder route to be
   * moved. Empty array denotes that no folder is to be moved.
   * @param dest_folder The full path of destination folder
   */
  void move(const dx::JSON &objects,
            const dx::JSON &folders,
            const std::string &dest_folder) const;
  /**
   * Clones the specified objects and/or folders to other
   * data container.
   *
   * @param objects a JSON array of object IDs to be cloned.
   * Empty array denotes that no object is to be cloned.
   * @param folders An array of strings of the folder route to be
   * clones. Empty array denotes that no folder is to be cloned.
   * @param dest_proj The project in which to clone.
   * @param dest_folder The full path of destination folder
   */
  void clone(const dx::JSON &objects,
             const dx::JSON &folders,
             const std::string &dest_proj,
             const std::string &dest_folder="/") const;

  // Folder specific

  /** 
   * Creates a new folder
   *
   * @param folder Full path of the new folder to be created in data container.
   * @param parents If true then parent folder will be created if they
   * do not exist.
   */
  void newFolder(const std::string &folder, bool parents=false) const;
  
  /**
   * Lists a folder of data container.
   * 
   * @param folder A string representing the full path to the folder to
   * be listed.
   *
   * @return A JSON hash of this form:
   * {"objects": [{"id": id-1}, {"id": id-2}, ...], "folders": ["full-path-to-folder-1",
   "full-path-to-folder-2", ...]}
   */
  dx::JSON listFolder(const std::string &folder="/") const;
  
  /** 
   * Move a folder to specified destination folder (including the objects
   * and subfolders it contains)
   *
   * @param folder Full path of the folder to be moved
   * @param dest_folder Full path of the destination folder
   */
  void moveFolder(const std::string &folder,
                  const std::string &dest_folder) const;

  /**
   * Removes a given folder and all it's data content
   * It must be either empty or recurse must be set to true in input params
   *
   * @param folder Full path of the folder to be removed
   * @param recurse Boolean indicating whether removal should propagate 
   * to its contained folders and objects
   */
  void removeFolder(const std::string &folder, const bool recurse=false) const;

  // Objects-specific
  
  /** 
   * Move objects to specified destination folder in same data container.
   * 
   * @param objects JSON array of object id's (strings) to be moved
   * @param dest_folder Full path of the destination folder
   */
  void moveObjects(const dx::JSON &objects,
                   const std::string &dest_folder) const {
    move(objects, dx::JSON(dx::JSON_ARRAY), dest_folder);
  }
  
  /**
   * Removes object(s) from the data container
   *
   * @param JSON array of object id's (strings) to be removed
   */
  void removeObjects(const dx::JSON &objects) const;
  
  /**
   * Clone object(s) from one data container to other.
   * 
   * @param objects JSON array of object id's (strings) to be cloned.
   * @param dest_proj Destination data container id.
   * @param dest_folder Id of the destination folder.
   */
  void cloneObjects(const dx::JSON &objects,
                    const std::string &dest_proj,
                    const std::string &dest_folder) const {
    clone(objects, dx::JSON(dx::JSON_ARRAY), dest_proj, dest_folder);
  }
};

class DXProject : public DXContainer {
 public:
 DXProject(const std::string &dxid=g_WORKSPACE_ID) : DXContainer(dxid) { }
  
  /**
   * Update the project with specified options.
   * See route: /project-xxxx/update for details.
   *
   * @param to_update JSON hash as expected by /project-xxxx/update route
   */
  void update(const dx::JSON &to_update) const;
  
  /**
   * Destroys the specified project. All objects are removed.
   * Any jobs running in the project context are terminated.
   */
  void destroy() const;

  /**
   * Invite other people (or PUBLIC) to the project.
   *
   * @param invitee Username or email of the person to be invited to the project.
   * Use "PUBLIC" to make it publically available.
   * @param level Permission level that the invitee would get ("LIST", "VIEW",
   * "CONTRIBUTE", "ADMINISTER")
   */
  void invite(const std::string &invitee, const std::string &level) const;
  
  /**
   * @param member Username of the project member whose permission will be
   * decreased
   * @param level The new permission level for the user ("LIST", "VIEW",
   * "CONTRIBUTE", "ADMINISTER")
   */
  void decreasePerms(const std::string &member, const std::string &level) const;
};

#endif
