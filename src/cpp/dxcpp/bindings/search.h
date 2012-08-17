#ifndef DXCPP_BINDINGS_SEARCH_H
#define DXCPP_BINDINGS_SEARCH_H

#include "../bindings.h"

/**
 * This class contain static member functions corrosponding to
 * /find* routes.
 *
 * For convenience of users, all methods of DXSystem class treat "timestamp" fields 
 * in input hash differently then apiserver. Unlike apiserver they allow the
 * field to be either of the two: integer or string (apiserver only allows
 * integer fields). For all non-negative timestamp values, DXSystem
 * methods behave exactly the way apiserver does. In general:
 * 
 * - In case of integer, non-negative timestamp denote the usual, i.e., 
 * number of milliseconds since unix epoch. But negative timestamp T, 
 * represent (Current time - |T|), i.e., those many milliseconds
 * before the current time.
 *
 * - In case of string, it must be of form: "Number-Suffix". For ex:
 * "1s" (denotes 1 second ahead from now), "-1s" (denotes 1 second before), etc.
 * Suffixes allowed are: "s", "m", "d", "w", or "y" (for seconds, minutes, days, weeks, 
 * or years). (Note: year is defined as 365 days).
 */
class DXSystem {
public:
  /**
   * This function is an easy wrapper for route: /system/findDataObjects
   *
   * @param query A JSON hash, as expected by the route: /system/findDataObjects
   * @return A JSON hash, exactly as returned by the route: /system/findDataObjects
   *
   * @note 
   * - Timestamp fields in input query are allowed to be more relaxed than what
   * the api route expects: See documentation at top of DXSystem class for details.
   * - If input query doesn't have field "scope", then all private objects are searched,
   * but otherwise if query["scope"] doesn't have field "project", then it is set to 
   * current Workspace ID (if available, else a DXError is thrown).
   */
  static dx::JSON findDataObjects(dx::JSON query);

  /**
   * Exactly same as findDataObjects(), except that only
   * top result is returned (or null if their are no results).
   * 
   * @see findDataObjects()
   * 
   * @param query A JSON hash, as expected by findDataObjects()
   * @return If at least one object matched the search criteria, then a JSON_HASH
   * containing following keys: "id", "project", "describe" (if asked for) will
   * be returned. If no object matched the search criteria, then JSON_NULL is returned.
   */
  static dx::JSON findOneDataObject(dx::JSON query);

  /**
   * This function is an easy wrapper for route: /system/findJobs
   *
   * @param query A JSON hash, as expected by the route: /system/findJobs
   * @return A JSON hash, exactly as returned by the route: /system/findJobs
   *
   * @note 
   * - Timestamp fields in input query are allowed to be more relaxed than what
   * the api route expects: See documentation at top of DXSystem class for details.
   */
  static dx::JSON findJobs(dx::JSON query);

  /**
   * This function is an easy wrapper for route: /system/findProjects
   *
   * @param query A JSON hash, as expected by the route: /system/findProjects
   * @return A JSON hash, exactly as returned by the route: /system/findProjects
   */
  static dx::JSON findProjects(dx::JSON query);

  /**
   * This function is an easy wrapper for route: /system/findApps
   *
   * @param query A JSON hash, as expected by the route: /system/findApps
   * @return A JSON hash, exactly as returned by the route: /system/findApps
   *
   * @note 
   * - Timestamp fields in input query are allowed to be more relaxed than what
   * the api route expects: See documentation at top of DXSystem class for details.
   */
  static dx::JSON findApps(dx::JSON query);
};
#endif
