// Copyright (C) 2013-2015 DNAnexus, Inc.
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
 * \brief Searching for objects in the Platform
 */

#ifndef DXCPP_BINDINGS_SEARCH_H
#define DXCPP_BINDINGS_SEARCH_H

#include "../bindings.h"

namespace dx {
  //! Static functions for searching for objects

  /**
   * This class contains static member functions corresponding to /find* routes.
   *
   * For your convenience, all methods of the %DXSystem class interpret "timestamp" fields in input
   * hashes more loosely then the raw API does. Unlike the raw API, any "timestamp" value may be an
   * integer or a string (the raw API only allows integer values). For all non-negative timestamp
   * values, %DXSystem methods behave exactly the way the raw API does. In general:
   *
   * - In case of an integer timestamp value, non-negative timestamps are interpreted in the normal
   *   way, i.e., as the number of milliseconds since the Unix epoch. However, negative timestamps T
   *   represent (current time - |T|), i.e., that many milliseconds before the current time.
   *
   * - A string timestamp value must be of the form: "Number-Suffix". For example, "1s" denotes 1
   *   second from now, while "-1s" denotes 1 second ago, etc. The allowed suffixes are: "s", "m",
   *   "d", "w", or "y" (for seconds, minutes, days, weeks, or years). A year is defined as 365
   *   days.
   */
  class DXSystem {
  public:
    /**
     * This function is a wrapper around the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Search#API-method%3A-%2Fsystem%2FfindDataObjects">/system/findDataObjects</a>
     * API method.
     *
     * @param query A JSON hash to be provided to /system/findDataObjects.
     * @return A JSON hash as returned by /system/findDataObjects.
     *
     * @note
     * - Timestamp fields in the input query are allowed to be more relaxed than what the raw API
     *   expects. See the documentation at top of the DXSystem class for details.
     * - If input query doesn't have the field "scope", then all private objects are searched. If
     *   query["scope"] is supplied but doesn't have the field "project", then it is set to the
     *   current Workspace ID (if this is not available, a DXError is thrown).
     */
    static dx::JSON findDataObjects(dx::JSON query);

    /**
     * Exactly the same as findDataObjects(), except that only the first result is returned (or null
     * if there are no results).
     *
     * @see findDataObjects()
     *
     * @param query A JSON hash, as expected by findDataObjects().
     *
     * @return If at least one object matched the search criteria, then a JSON_HASH containing the
     * following keys: "id", "project", "describe" (if requested) will be returned. If no object
     * matched the search criteria, then JSON_NULL is returned.
     */
    static dx::JSON findOneDataObject(dx::JSON query);

    /**
     * This function is a wrapper around the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Search#API-method%3A-%2Fsystem%2FfindJobs">/system/findJobs</a>
     * API method.
     *
     * @param query A JSON hash to be provided to /system/findJobs.
     * @return A JSON hash as returned by /system/findJobs.
     *
     * @note
     * - Timestamp fields in the input query are allowed to be more relaxed than what the raw API
     *   expects. See the documentation at top of the DXSystem class for details.
     */
    static dx::JSON findJobs(dx::JSON query);

    /**
     * This function is a wrapper around the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Search#API-method%3A-%2Fsystem%2FfindProjects">/system/findProjects</a>
     * API method.
     *
     * @param query A JSON hash to be provided to /system/findProjects.
     * @return A JSON hash as returned by /system/findProjects.
     */
    static dx::JSON findProjects(dx::JSON query);

    /**
     * This function is a wrapper around the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Search#API-method%3A-%2Fsystem%2FfindApps">/system/findApps</a>
     * API method.
     *
     * @param query A JSON hash to be provided to /system/findApps.
     * @return A JSON hash as returned by /system/findApps.
     *
     * @note
     * - Timestamp fields in the input query are allowed to be more relaxed than what the raw API
     *   expects. See the documentation at top of the DXSystem class for details.
     */
    static dx::JSON findApps(dx::JSON query);
  };
}
#endif
