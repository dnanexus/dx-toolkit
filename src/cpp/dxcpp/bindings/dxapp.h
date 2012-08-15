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
   * Sets the ID or name of the app, along with the version or tag if
   * appropriate.
   *
   * @param nameOrID Either the name of the app, (e.g. "micromap"), or the object ID of the app version (e.g. "app-j47b1k3z8Jqqv001213v312j1")
   * @param alias If nameOrID is an app name, then this field will be interpreted as the version or tag of the app to be used
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
  
  /** 
   * Returns the output JSON hash of /app-xxx/describe call
   * 
   * @return Output of describe call on the app.
   */
  dx::JSON describe() const;

  /**
   * Updates the app with given input hash.
   * See /app-xxx[/yyyy]/update for details.
   */
  void update(const dx::JSON &to_update) const;

  /**
   * Add categories to the app.
   *
   * @param categories A JSON array of strings, each of which will be added
   * as a category to the app.
   */
  void addCategories(const dx::JSON &categories) const;
  
  /**
   * Remove categories from the app.
   *
   * @param categories A JSON array of strings, each of which will be removed
   * as a category from the app.
   */
  void removeCategories(const dx::JSON &categories) const;

  /**
   * Add tags to the app.
   *
   * @param tags A JSON array of strings, each of which will be added
   * as a tag to the app.
   */
  void addTags(const dx::JSON &tags) const;

  /**
   * Remove tags from the app.
   *
   * @param tags A JSON array of strings, each of which will be removed
   * as a tag from the app.
   */
  void removeTags(const dx::JSON &tags) const;
 
  /**
   * Installs the app into user's account
   * See route: /app-xxxx[/yyyy]/install
   */
  void install() const;
  
  /**
   * Uninstalls the app from user's account.
   * No error is thrown if the app wasn't originalyl installed.
   * See route: /app-xxxx[/yyyy]/uninstall
   */
  void uninstall() const;
 
  /**
   * Returns the full specification of the app as a JSON object.
   * See route: /app-xxxx[/yyyy]/get for details.
   *
   * @return Full specification of the app
   */
  dx::JSON get() const;

  /**
   * Makes this version of app discoverable by
   * other users on DNAnexus platform.
   *
   * See route: /app-xxxx[/yyyy]/publish for details
   *
   * @param makeDefault If true, then makes this version
   * of the app "default".
   */
  void publish(bool makeDefault=false) const;

  /** 
   * The app is now "deleted" and can no longer be run, modified,
   * or published. This state is reflected in output of describe.
   *
   * See route: /app-xxxx[/yyyy]/delete
   */
  void remove() const;

  /** 
   * Runs this app and returns handler to job created by the "run"
   *
   * See route: /app-xxxx/[/yyyy]/run
   *
   * @param app_input A hash of key/value pairs representing the input
   * that app is launched with
   * @param project_context A String representing the project in whose
   * context the app would be run
   * @param output_folder A string representing the folder in which objects
   * outputted by the app run will be placed.
   */
  DXJob run(const dx::JSON &app_input,
            const std::string &project_context=g_WORKSPACE_ID,
            const std::string &output_folder="/") const;
};

#endif
