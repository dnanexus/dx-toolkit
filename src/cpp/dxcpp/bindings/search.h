#ifndef DXCPP_BINDINGS_SEARCH_H
#define DXCPP_BINDINGS_SEARCH_H

#include "../bindings.h"

class DXSystem {
public:
  /**
   * This function is an easy wrapper over /system/findDataObjects route.
   * The input to this function is a JSON hash, whose fields determine constraints
   * to be used while querying. The format of the input JSON hash is similar to the
   * /system/findDataObjects input (TODO: Add link for api doc here)
   *
   * For convenience of users, we treat "timestamp" fields in input hash
   * slightly differently. We assume negative timestamps T, represent: 
   * (Current time - |T|), i.e., those many milliseconds
   * before the current time. We also allow strings in timestamp field with suffixes: 
   * "s", "m", "d", "w", or "y" (for seconds, minutes, days, weeks, or years). For example,
   * "-1w", represent timestamp one week before current time.
   *
   * TODO: Add details about exceptions thrown (like invalid input for timestamp, etc
   * TODO: Do we want to add a default project context ?
   * @param query A JSON hash containing the query for /findDataObjects. For details
   * on possible fields, please see API Doc.
   * @return A JSON hash, as would be returned by the api route.
   */
  static dx::JSON findDataObjects(const dx::JSON &query);

  /** 
   * This function return the first data object matching the query
   * The query language is same as for findDataObjects()
   * TODO: Put the timestamp definition at a common place, and reference it everywhere
   *
   * @param query A JSON hash containing the query.
   * @return If at least one object matched the search criteria, then a JSON_HASH
   * containing following keys: "id", "project", "describe" (if asked for) will
   * be returned. If no object matched the search criteria, then JSON_NULL is returned.
   */
  static dx::JSON findOneDataObject(const dx::JSON &query);

  //TODO: Documentation
  static dx::JSON findJobs(const dx::JSON &query);

  //TODO: Documentation
  static dx::JSON findProjects(const dx::JSON &query);

  //TODO: Documentation
  static dx::JSON findApps(const dx::JSON &query);
};
#endif
