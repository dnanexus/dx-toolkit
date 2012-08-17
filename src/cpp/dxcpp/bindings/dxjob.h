#ifndef DXCPP_BINDINGS_DXJOB_H
#define DXCPP_BINDINGS_DXJOB_H

#include <string>
#include <limits>
#include "dxjson/dxjson.h"

class DXJob {
 private:
  std::string dxid_;
 public:
  DXJob() { }
  DXJob(const std::string &dxid) : dxid_(dxid) { }

  /**
   * Returns the output of /job-xxxx/describe call
   *
   * @return Output hash of describe call
   */
  dx::JSON describe() const;

  /**
   * Resets the handler to refer to specified job id.
   *
   * @param dxid ID of the job to be pointed by handler
   */
  void setID(const std::string &dxid) { dxid_ = dxid; }
 
  /** 
   * Return the current job state.
   * @return state of the job
   */
  std::string getState() const { return describe()["state"].get<std::string>(); }

  /**
   * Gets the ID of the job the handler is pointing to
   *
   * @return A string representing ID of a job or blank string
   * if the handler is associated to no job.
   */
  std::string getID() const { return dxid_; }
  
  /**
   * Creates a new job which will execute a particular function form
   * the same applet as the current job is running with a particular
   * input. Resets the handler to point to new job.
   *
   * @note This function can only be called from within an executing job.
   *
   * @param fn_input A hash of key/value pairs. This is a freeform JSON hash
   * that will be passed verbatim to the job, after it is checked for the
   * presence of links
   * @param fn_name Name of the function in applet
   * code that should be executed in beginning of the job.
   * @param job_name Name for the resulting job. If blank string is given,
   * then (default name of parent job + ":fn_name") will be used instead.
   * @param resources A hash specifying the minimum resources to be allocated
   * to this job. Please see route: /job-xxxx/new for details about this hash.
   */
  void create(const dx::JSON &fn_input, const std::string &fn_name, const std::string &job_name="", const dx::JSON resources=dx::JSON(dx::JSON_NULL));

  /**
   * Terminates the job and all it's descendant jobs.
   * This call is only valid for a job which has no parent.
   */
  void terminate() const;

  /** 
   * This function blocks until either the job is in "done" state
   * ,or, the given timeout value is exceeded.
   *
   * @param timeout Maximum number of seconds to wait for job to move into
   * "done" state. (Default ~= Infinity ( > 60yrs)).
   *
   * Note: The actual timeout value in practise can be upto 2 seconds larger.
   * @warning If the job reaches into failed state, and no timeout value was provided
   * then this function will block forever. Use with caution.
   */
  void waitOnDone(const int timeout=std::numeric_limits<int>::max()) const;
};

#include "../bindings.h"

#endif
