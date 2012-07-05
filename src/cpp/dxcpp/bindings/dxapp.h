#ifndef DXCPP_BINDINGS_DXAPP_H
#define DXCPP_BINDINGS_DXAPP_H

#include "../bindings.h"

class DXApp {
private:
  std::string dxid_;
  std::string name_;
  std::string alias_;

public:
  // App-specific functions
  DXApp() { }
  DXApp(const std::string &nameOrID,
        const std::string &alias="default") {
    setID(nameOrID, alias);
  }

  /**
   * @param nameOrID Either the name of the app, (e.g. "micromap"), or the object ID of the app version (e.g. "app-j47b1k3z8Jqqv001213v312j1")
   * @param alias If nameOrID is an app name, then this field will be interpreted as the version or tag of the app to be used
   *
   * Sets the ID or name of the app, along with the version or tag if
   * appropriate.
   */
  void setID(const std::string &nameOrID, const std::string &alias="default") {
    if (nameOrID.find("app-") == 0) {
      dxid_ = nameOrID;
      name_ = "";
      alias_ = "";
    } else {
      dxid_ = "";
      name_ = nameOrID;
      alias_ = alias;
    }
  }

  dx::JSON describe() const;

  void addCategories(const dx::JSON &categories) const;

  void removeCategories(const dx::JSON &categories) const;

  void addTags(const dx::JSON &tags) const;

  void removeTags(const dx::JSON &tags) const;

  DXJob run(const dx::JSON &app_input,
            const std::string &project_context=g_WORKSPACE_ID,
            const std::string &output_folder="/") const;
};

#endif
