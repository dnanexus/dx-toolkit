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

#ifndef __DXCPP_UTILS_H__
#define __DXCPP_UTILS_H__

#include <string>
#include <vector>

namespace dx {
  /** @internal
   */

  std::string getUserHomeDirectory();

  std::string joinPath(const std::string &first_path,
                       const std::string &second_path,
                       const std::string &third_path="");

  // Returns MD5 hash (as a hex string) for the given vector<char>
  std::string getHexifiedMD5(const std::vector<char> &inp);
  std::string getHexifiedMD5(const unsigned char *ptr, const unsigned long size);
  std::string getHexifiedMD5(const std::string &inp);

  namespace _internal {
    // Sleep for "duration" seconds using nanosleep (for posix systems)
    // and using boost::this_thread::sleep() on windows
    int sleepUsingNanosleep(unsigned int sec);
  }
}

#endif
