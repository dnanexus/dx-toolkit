#ifndef DXCPP_H
#define DXCPP_H

#include <stdlib.h>
#include <stdint.h>
#include <map>
#include <string>
#include "dxjson/dxjson.h"
#include "exceptions.h"

/**
 * Default project or workspace ID to access.
 */
extern std::string g_WORKSPACE_ID;

/**
 * Job ID if running in an execution environment.
 */
extern std::string g_JOB_ID;

/**
 * Project ID of the project context.  Applicable only for execution
 * as a job run from a particular project.
 */
extern std::string g_PROJECT_CONTEXT_ID;

extern std::string g_APISERVER_PROTOCOL;
extern std::string g_APISERVER_HOST;
extern std::string g_APISERVER_PORT;
extern std::string g_APISERVER;

extern dx::JSON g_SECURITY_CONTEXT;

extern bool g_APISERVER_SET;
extern bool g_SECURITY_CONTEXT_SET;

/**
 * This is a low-level function for making an HTTP request to the API
 * server using the information that has been set by
 * setAPIServerInfo() and setSecurityContext().
 *
 * @param resource API server route to access, e.g. "/file/new"
 * @param data Data to send in the request
 * @param alwaysRetry If true, then a failed request is <b>always</b> retried. Should be set to true for <b>only</b> idempotent requests. Default value = false.
 * @param headers Additional HTTP headers to include in the request
 * @return The response from the API server, parsed as a JSON
 */
dx::JSON DXHTTPRequest(const std::string &resource, const std::string &data, const bool alwaysRetry = false,
                       const std::map<std::string, std::string> &headers = std::map<std::string, std::string>());

/**
 * Sets the information used to contact the API server for use by
 * DXHTTPRequest().
 *
 * @param host API server hostname
 * @param port Port number
 * @param protocol Protocol to use, e.g. "http"
 */
void setAPIServerInfo(const std::string &host, int port, const std::string &protocol);

/**
 * Sets the security context for constructing the necessary headers in
 * DXHTTPRequest().
 *
 * @param security_context A JSON object with keys "auth_token_type"
 * and "auth_token"
 */
void setSecurityContext(const dx::JSON &security_context);

/**
 * Sets the default project or workspace ID to use when creating
 * object or project handlers.
 *
 * @param workspace_id Project or workspace ID
 */
void setWorkspaceID(const std::string &workspace_id);

/**
 * Sets the job ID.
 *
 * @param job_id Job ID
 */
void setJobID(const std::string &job_id);

/**
 * Sets the project context ID
 *
 * @param project_id Project ID
 */
void setProjectContext(const std::string &project_id);

/**
 * Loads the data from environment variables and calls setAPIServerInfo(),
 * setSecurityContext(), setWorkspaceID(), and setProjectContext() as
 * appropriate.
 */
bool loadFromEnvironment();

#include "api.h"
#include "bindings.h"

#endif
