#ifndef DXCPP_BINDINGS_DXPROJECT_H
#define DXCPP_BINDINGS_DXPROJECT_H

#include "../bindings.h"

class DXProject {
 private:
  std::string dxid_;
 public:
 DXProject(const std::string &dxid=g_WORKSPACE_ID) : dxid_(dxid) { }

  void setID(const std::string &dxid) { dxid_ = dxid; }
  /**
   * @return ID of the associated data object
   */
  std::string getID() const { return dxid_; }
  /**
   * Default conversion to string is to its project ID so a handler
   * can always be passed in place of a string argument that expects
   * a project ID.
   */
  operator std::string() { return dxid_;}

  dx::JSON describe(bool folders=false) const;
  void update(const dx::JSON &to_update) const;
  void destroy() const;

  // Generic
  void move(const dx::JSON &objects,
            const dx::JSON &folders,
            const std::string &dest_folder) const;
  void clone(const dx::JSON &objects,
             const dx::JSON &folders,
             const std::string &dest_proj,
             const std::string &dest_folder="/") const;

  // Folder-specific
  void newFolder(const std::string &folder, bool parents=false) const;
  dx::JSON listFolder(const std::string &folder="/") const;
  void moveFolder(const std::string &folder,
                  const std::string &dest_folder) const;
  void removeFolder(const std::string &folder, const bool recurse=false) const;

  // Objects-specific
  void moveObjects(const dx::JSON &objects,
                   const std::string &dest_folder) const {
    move(objects, dx::JSON(dx::JSON_ARRAY), dest_folder);
  }
  void removeObjects(const dx::JSON &objects) const;
  void cloneObjects(const dx::JSON &objects,
                    const std::string &dest_proj,
                    const std::string &dest_folder) const {
    clone(objects, dx::JSON(dx::JSON_ARRAY), dest_proj, dest_folder);
  }

  void invite(const std::string &invitee, const std::string &level) const;
  void decreasePerms(const std::string &member, const std::string &level) const;
};

#endif
