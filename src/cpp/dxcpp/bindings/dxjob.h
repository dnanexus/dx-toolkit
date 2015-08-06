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
 * \brief Jobs.
 */

#ifndef DXCPP_BINDINGS_DXJOB_H
#define DXCPP_BINDINGS_DXJOB_H

#include <string>
#include <limits>
#include "dxjson/dxjson.h"

namespace dx {
  //! The instantiation of an applet or app.

  ///
  /// When a user runs an applet or app (for example with DXApplet::run or DXApp::run), a job object
  /// is created in the system; the job is then executed on a worker node.
  ///
  /// A job may launch other jobs (for example, to parallelize its work). To facilitate this kind of
  /// pattern, an applet or app may define multiple entry points. The job may run a different entry
  /// point associated with the same applet or app, or another applet or app entirely.
  ///

  class DXJob {
   private:
    std::string dxid_;
   public:
    DXJob() { }

    /**
     * Creates a %DXJob handler for the specified remote job.
     *
     * @param dxid Job ID.
     */
    DXJob(const std::string &dxid) : dxid_(dxid) { }

    /**
     * Returns a description of the job.
     *
     * @return JSON hash describing the job, as given by the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets-and-Entry-Points#API-method%3A-%2Fjob-xxxx%2Fdescribe">/job-xxxx/describe</a>
     * API method.
     */
    dx::JSON describe() const;

    /**
     * Updates the handler to refer to the specified remote job ID.
     *
     * @param dxid Job ID.
     */
    void setID(const std::string &dxid) { dxid_ = dxid; }

    /**
     * Returns the current remote job state.
     *
     * @return A string containing the current state of the job. Possible values are listed in the <a
     * href="https://wiki.dnanexus.com/Job-Lifecycle">API
     * specification</a>.
     */
    std::string getState() const { return describe()["state"].get<std::string>(); }

    /**
     * Returns the ID of the associated remote job.
     *
     * @return A string giving the ID of a job, or blank string if the handler is associated with no
     * job.
     */
    std::string getID() const { return dxid_; }

    /**
     * Creates a new job whose entry point is any function in the currently running app or applet.
     * The specified input is provided to the new job. The handler is updated to point to the ID of
     * the newly created job.
     *
     * @note This function can only be called from within a currently running job.
     *
     * @param fn_input A hash of key/value pairs. This is a freeform JSON hash that will be passed
     * verbatim to the job, after it is checked for the presence of links.
     * @param fn_name Name of the function (in the current app or applet) to use as the entry point.
     * @param job_name Name for the resulting job. If blank string is given, then (default name of
     * parent job + ":fn_name") will be used instead.
     * @param depends_on A list of job IDs and/or data object IDs (string), representing jobs that must finish and/or data objects that must close before this job should start running.
     * @param instance_type A string, or a JSON_HASH (values must be string), representing instance type on which the job will 
     * be run, or a mapping of function names to instance types. (Note: you can pass a 
     * std::map<string, string> as well)
     */
    void create(const dx::JSON &fn_input, 
                const std::string &fn_name, 
                const std::string &job_name="",
                const std::vector<std::string> &depends_on=std::vector<std::string>(),
                const dx::JSON &instance_type=dx::JSON(dx::JSON_NULL)
                );

    /**
     * Terminates the job and all its descendant jobs.
     *
     * This call is only valid for a job which has no parent.
     */
    void terminate() const;

    /**
     * This function blocks until the associated job is in the "done" state, or the specified timeout
     * value is exceeded.
     *
     * @param timeout Maximum number of seconds to wait for job to move into "done" state. (Default
     * ~= Infinity ( > 60yrs)).
     *
     * @note The actual timeout value in practice can be up to 2 seconds larger.
     *
     * @warning If the job the failed state, and no timeout value was provided, then this function
     * will block forever. Use with caution.
     */
    void waitOnDone(const int timeout=std::numeric_limits<int>::max()) const;

    /**
     * Constructs a JSON job-based object reference using the stored job
     * ID and the given output field name.  The JSON will be of the form
     *
     * {"job": <job ID>, "field", <job output field name>}
     *
     * @param field The output field name to be referenced
     *
     * @return JSON of the requested job-based object reference
     */
    dx::JSON getOutputRef(const std::string &field);

    /**
     * Creates a new job whose entry point is any function in the currently running app or applet.
     * The specified input is provided to the new job. The handler is updated to point to the ID of
     * the newly created job.
     *
     * @note This function can only be called from within a currently running job.
     *
     * @param fn_input A hash of key/value pairs. This is a freeform JSON hash that will be passed
     * verbatim to the job, after it is checked for the presence of links.
     * @param fn_name Name of the function (in the current app or applet) to use as the entry point.
     * @param job_name Name for the resulting job. If blank string is given, then (default name of
     * parent job + ":fn_name") will be used instead.
     * @param depends_on A list of Job ID's (string), representing jobs that must finish before this job should start running.
     * @param instance_type A string, or a JSON_HASH (values must be string), representing instance type on which the job will 
     * be run, or a mapping of function names to instance types. (Note: you can pass a 
     * std::map<string, string> as well)
     *
     * @return A DXJob handler for the newly created job.
     */
    static DXJob newDXJob(const dx::JSON &fn_input,
                          const std::string &fn_name,
                          const std::string &job_name="",
                          const std::vector<std::string> &depends_on=std::vector<std::string>(),
                          const dx::JSON &instance_type=dx::JSON(dx::JSON_NULL)
                          );
  };
}

#include "../bindings.h"

#endif
