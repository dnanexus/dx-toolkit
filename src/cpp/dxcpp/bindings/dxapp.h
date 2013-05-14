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

/** \file
 *
 * \brief Apps.
 */

//! An executable object that can be published for others to discover.

///
/// Apps allow users and developers to publish their software for everyone on the platform. Apps
/// extend the functionality of applets (represented by DXApplet objects) to allow reproducibility,
/// versioning, collaborative development, and community feedback.
///
/// Unlike applets, apps are published in a single global namespace.
///
/// The %DXApp handler allows you to read and modify the metadata of existing app objects in the
/// Platform. To create a new app object, use the <code>dx-build-app</code> command-line tool in
/// the DNAnexus SDK.
///
/// See <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps">Apps</a> in the API
/// specification for more information.
///

#ifndef DXCPP_BINDINGS_DXAPP_H
#define DXCPP_BINDINGS_DXAPP_H

#include "../bindings.h"

namespace dx {
  class DXApp {
  private:
    std::string dxid_;
    std::string name_;
    std::string alias_;

  public:
    // App-specific functions
    DXApp() { }

    /**
     * Creates a handler for the remote app version. Providing only an app name (such as "micromap")
     * selects the version of the app that is tagged "default". You can select an arbitrary version
     * by providing a unique identifier (e.g. "app-j47b1k3z8Jqqv001213v312j1") or a combination of a
     * name and version (e.g. "micromap" "1.0.1").
     *
     * @param nameOrID Either the name of the app, (e.g. "micromap"), or the object ID of the app version (e.g. "app-j47b1k3z8Jqqv001213v312j1")
     * @param alias The version or tag of the app to be used (if nameOrID is an app name)
     */
    DXApp(const std::string &nameOrID,
          const std::string &alias="default") {
      setID(nameOrID, alias);
    }

    /**
     * Sets the app ID to that of a different remote app version. Providing only an app name (such as
     * "micromap") selects the version of the app that is tagged "default". You can select an
     * arbitrary version by providing a unique identifier (e.g. "app-j47b1k3z8Jqqv001213v312j1") or a
     * combination of a name and version (e.g. "micromap" "1.0.1").
     *
     * @param nameOrID Either the name of the app, (e.g. "micromap"), or the object ID of the app version (e.g. "app-j47b1k3z8Jqqv001213v312j1")
     * @param alias The version or tag of the app to be used (if nameOrID is an app name)
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
     * Returns a description of the app, as specified by the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2Fdescribe">/app-xxxx[/yyyy]/describe</a>
     * API method.
     *
     * @return JSON hash containing the describe output
     */
    dx::JSON describe() const;

    /**
     * Updates the remote app's properties with given input hash, as specified by the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2Fupdate">/app-xxxx[/yyyy]/update</a>
     * API method.
     */
    void update(const dx::JSON &to_update) const;

    /**
     * Adds the specified categories to the app. (Setting categories affects all versions of this app.)
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2FaddCategories">/app-xxxx[/yyyy]/addCategories</a>
     * API method for more info.
     *
     * @param categories A JSON array of strings, each of which will be added
     * as a category to the app.
     */
    void addCategories(const dx::JSON &categories) const;

    /**
     * Removes the specified categories from the app. (Setting categories affects all versions of this app.)
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2FremoveCategories">/app-xxxx[/yyyy]/removeCategories</a>
     * API method for more info.
     *
     * @param categories A JSON array of strings, each of which will be removed
     * as a category from the app.
     */
    void removeCategories(const dx::JSON &categories) const;

    /**
     * Adds the specified tags to the app.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2FaddTags">/app-xxxx[/yyyy]/addTags</a>
     * API method for more info.
     *
     * @param tags A JSON array of strings, each of which will be added
     * as a tag to the app.
     */
    void addTags(const dx::JSON &tags) const;

    /**
     * Removes the specified tags from the app.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2FremoveTags">/app-xxxx[/yyyy]/removeTags</a>
     * API method for more info.
     *
     * @param tags A JSON array of strings, each of which will be removed
     * as a tag from the app.
     */
    void removeTags(const dx::JSON &tags) const;

    /**
     * Installs the app into the requesting user's account.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2Finstall">/app-xxxx[/yyyy]/install</a>
     * API method for more info.
     */
    void install() const;

    /**
     * Uninstalls the app from the requesting user's account, if it was previously installed. No
     * error is thrown if the app wasn't originally installed.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2Funinstall">/app-xxxx[/yyyy]/uninstall</a>
     * API method for more info.
     */
    void uninstall() const;

    /**
     * Returns the full specification of the app as a JSON object, as described in the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2Fget">/app-xxxx[/yyyy]/get</a>
     * API method.
     *
     * @return JSON has containing the full specification of the app
     */
    dx::JSON get() const;

    /**
     * Makes this version of app discoverable by other users on the DNAnexus platform.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2Fpublish">/app-xxxx[/yyyy]/publish</a>
     * API method for more info.
     *
     * @param makeDefault If true, then also makes this version of the app the
     * "default".
     */
    void publish(bool makeDefault=false) const;

    /**
     * Deletes the app so that it can no longer be run, modified, or published. This state is
     * reflected in output of DXApp::describe.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2Fdelete">/app-xxxx[/yyyy]/delete</a>
     * API method for more info.
     */
    void remove() const;

    /**
     * Runs this app with the specified input and returns a handler for the resulting job.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp-xxxx%5B%2Fyyyy%5D%2Frun">/app-xxxx[/yyyy]/run</a>
     * API method for more info.
     *
     * @param app_input A hash of name/value pairs specifying the input
     * that the app is to be launched with.
     * @param output_folder The folder (within the project_context) in which the app's output objects will be placed.
     * @param depends_on A list of job IDs and/or data object IDs (string), representing jobs that must finish and/or data objects that must close before this job should start running.
     * @param instance_type A string, or a JSON_HASH (values must be string), representing instance type on which the job with 
     * the entry point "main" will be run, or a mapping of function names to instance types. (Note: you can pass a 
     * std::map<string, string> as well)
     * @param project_context The project context in which the app is to be run (used *only* if called outside of a running job, i.e., DX_JOB_ID is not set)
     *
     * @return Handler for the job that was launched.
     */
    DXJob run(const dx::JSON &app_input,
              const std::string &output_folder="/", 
              const std::vector<std::string> &depends_on=std::vector<std::string>(),
              const dx::JSON &instance_type=dx::JSON(dx::JSON_NULL),
              const std::string &project_context=config::CURRENT_PROJECT()
              ) const;
  };
}
#endif
