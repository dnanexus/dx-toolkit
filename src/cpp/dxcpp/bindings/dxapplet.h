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
 * \brief Applets.
 */

//! An executable object representing an analysis or other piece of software.

///
/// An applet operates on input data and produces output data. Both the inputs and the outputs of
/// an applet can include a combination of simple objects (of numeric, string, hash, or boolean
/// type; passed by value) or data objects (passed by reference).
///
/// To publish your software for public consumption, create an App object (represented by DXApp)
/// instead.
///
/// To create a new applet object, consider using the <code>dx build</code> command-line
/// tool in the DNAnexus SDK.
///
/// See <a href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets">Applets</a> in the
/// API specification for more information.
///

#ifndef DXCPP_BINDINGS_DXAPPLET_H
#define DXCPP_BINDINGS_DXAPPLET_H

#include "../bindings.h"

namespace dx {
  class DXApplet: public DXDataObject {
  private:
    dx::JSON describe_(const std::string &s)const{return appletDescribe(dxid_,s);}
    void addTypes_(const std::string &UNUSED(s))const{throw DXNotImplementedError("Wrapper for /applet-xxxx/addTypes does not exist");}
    void removeTypes_(const std::string &UNUSED(s))const{throw DXNotImplementedError("Wrapper for /applet-xxxx/removeTypes does not exist");}
    dx::JSON getDetails_(const std::string &s)const{return appletGetDetails(dxid_,s);}
    void setDetails_(const std::string &UNUSED(s))const{throw DXNotImplementedError("Wrapper for /applet-xxxx/setDetails does not exist");}
    void setVisibility_(const std::string &UNUSED(s))const{throw DXNotImplementedError("Wrapper for /applet-xxxx/setVisibility does not exist");}
    void rename_(const std::string &s)const{appletRename(dxid_,s);}
    void setProperties_(const std::string &s)const{appletSetProperties(dxid_,s);}
    void addTags_(const std::string &s)const{appletAddTags(dxid_,s);}
    void removeTags_(const std::string &s)const{appletRemoveTags(dxid_,s);}
    void close_(const std::string &UNUSED(s))const{throw DXNotImplementedError("Wrapper for /applet-xxxx/close does not exist");}
    dx::JSON listProjects_(const std::string &s)const{return appletListProjects(dxid_,s);}

  public:
    // Note: We do not provide applet creation function .. since we want users
    // to use applet_builder for that task.

    // Applet-specific functions
    DXApplet() { }

    /**
     * Creates a %DXApplet handler for the specified remote applet.
     *
     * @param dxid Applet ID.
     * @param proj ID of the project in which to access the object (if NULL, then default workspace will be used).
     */
    DXApplet(const char *dxid, const char *proj=NULL) {
      setIDs(std::string(dxid), (proj == NULL) ? config::CURRENT_PROJECT() : std::string(proj));
    }
   
    /**
     * Creates a %DXApplet handler for the specified remote applet.
     *
     * @param dxid applet ID
     * @param proj ID of the project in which the applet should be accessed
     */
    DXApplet(const std::string &dxid,
             const std::string &proj=config::CURRENT_PROJECT()) { setIDs(dxid, proj); }
    
    /**
     * Creates a %DXApplet handler for the specified remote applet.
     *
     * @param dxlink A JSON representing a <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Details-and-Links#Linking">DNAnexus link</a>.
     *  You may also use the extended form: {"$dnanexus_link": {"project": proj-id, "id": obj-id}}.
     */
    DXApplet(const dx::JSON &dxlink) { setIDs(dxlink); }

    /**
     * Creates a new remote applet with the input hash, as specified in the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets-and-Entry-Points#API-method:-/applet/new">/applet/new</a>
     * API method.
     *
     * If <code>inp["project"]</code> is missing, then <code>config::CURRENT_PROJECT()</code> will be used as
     * the destination project.
     *
     * @param inp JSON hash representing the applet to be created
     */
    void create(dx::JSON inp);

    /**
     * Runs this applet with the specified input and returns a handler for the resulting job.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets-and-Entry-Points#API-method:-/applet-xxxx/run">/applet-xxxx/run</a>
     * API method for more info.
     *
     * @param applet_input A hash of name/value pairs specifying the input that the app is to be launched with.
     * @param output_folder The folder (within the project_context) in which the applet's output objects will be placed.
     * @param depends_on A list of job IDs and/or data object IDs (string), representing jobs that must finish and/or data objects that must close before this job should start running.
     * @param instance_type A string, or a JSON_HASH (values must be string), representing instance type on which the job with 
     * the entry point "main" will be run, or a mapping of function names to instance types. (Note: you can pass a 
     * std::map<string, string> as well)
     * @param project_context A string representing the project context in which the applet is to be run (used *only* if called outside of a running job, i.e., DX_JOB_ID is not set)
     *
     * @return Handler for the job that was launched.
     */
    DXJob run(const dx::JSON &applet_input,
              const std::string &output_folder="/", 
              const std::vector<std::string> &depends_on=std::vector<std::string>(),
              const dx::JSON &instance_type=dx::JSON(dx::JSON_NULL),
              const std::string &project_context=config::CURRENT_PROJECT()
              ) const;

    /**
     * Clones the applet into the specified project and folder.
     *
     * @param dest_proj_id ID of the project to which the object should be cloned
     * @param dest_folder Folder route in destination project into which the clone should be placed.
     *
     * @return New object handler with the associated project set to dest_proj_id.
     */
    DXApplet clone(const std::string &dest_proj_id,
                   const std::string &dest_folder="/") const;

    /**
     * Returns the full specification of the applet, as specified in the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets-and-Entry-Points#API-method:-/applet-xxxx/get">/applet-xxxx/get</a>
     * API method.
     *
     * @return JSON hash containing the full specification of the applet
     */
    dx::JSON get() const { return appletGet(dxid_); }

  };
}
#endif
