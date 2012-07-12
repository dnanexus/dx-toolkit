#ifndef DXCPP_BINDINGS_SEARCH_H
#define DXCPP_BINDINGS_SEARCH_H

#include "../bindings.h" // probably necessary??

/**
 * Please see the API documentation for details on the full list of
 * accepted fields in the query JSON: "class", "state", "visibility",
 * "name", "properties", "type", "tag", "link", "scope", "modified",
 * "created", "describe", "starting", "limit", and "level"
 *
 * @param query A JSON object containing the query by which data objects should be found.
 */
void findDataObjects(const dx::JSON &query);

void findOneDataObject(const dx::JSON &query);

void findJobs(const dx::JSON &query);

void findProjects(const dx::JSON &query);

void findApps(const dx::JSON &query);

#endif
