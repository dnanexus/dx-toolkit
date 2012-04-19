#ifndef DXCPP_BINDINGS_DXPROJECT_H
#define DXCPP_BINDINGS_DXPROJECT_H

#include "../bindings.h"

class DXProject {
 private:
  std::string dxid_;
 public:
 DXProject(const std::string &dxid=g_WORKSPACE_ID) : dxid_(dxid) { }

  void setID(const std::string &dxid) { dxid_ = dxid; }
  std::string getID() const { return dxid_; }

  dx::JSON describe() const;
  void update(const dx::JSON &to_update) const;
  void destroy() const;

  // Generic
  void move(const dx::JSON &objects,
            const dx::JSON &folders,
            const std::string &dest_folder) const;
  void clone(const dx::JSON &objects,
             const dx::JSON &folders,
             const std::string &dest_proj,
             const std::string &dest_folder) const;

  // Folder-specific
  void newFolder(const std::string &folder, bool parents) const;
  dx::JSON listFolder(const std::string &folder) const;
  void moveFolder(const std::string &folder,
                  const std::string &dest_folder) const;
  void removeFolder(const std::string &folder) const;

  // Objects-specific
  void moveObjects(const dx::JSON &objects,
                   const std::string &dest_folder) const {
    move(objects, dx::JSON(), dest_folder);
  }
  void removeObjects(const dx::JSON &objects) const;
  void cloneObjects(const dx::JSON &objects,
                    const std::string &dest_proj,
                    const std::string &dest_folder) const {
    clone(objects, dx::JSON(), dest_proj, dest_folder);
  }
};

#endif
