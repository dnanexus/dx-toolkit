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

#ifndef DXCPP_LOG_H
#define DXCPP_LOG_H

#include <sstream>

#define DXLOG(level) \
if (level < dx::Log::ReportingLevel()) ; \
else dx::Log().Get(level)

namespace dx {
  /*
   * Trivial support for logging to stderr in a thread-safe manner (logging
   * is atomic at the level of a chain of insertion (<<) operations).
   *
   * Profoundly inspired from: http://www.drdobbs.com/cpp/201804215.
   */
   enum LogLevel {
    logDEBUG4,
    logDEBUG3,
    logDEBUG2,
    logDEBUG1,
    logDEBUG,
    logINFO, // Default priority for a log message
    logWARNING,
    logERROR, // Highest priority log message
    DISABLE_LOGGING = 15// If ReportingLevel is set to this, nothing is logged
  };

  class Log {
  public:
    Log() { }
    ~Log();
    std::ostringstream& Get(LogLevel l=logINFO);
  public:
    static LogLevel& ReportingLevel();
    static std::string ToString(LogLevel level);
    static void Init();
  private:
    //disallow copy constructor & operator =()
    Log(const Log&);
    Log& operator=(const Log&);
  protected:
    std::ostringstream oss;
  };
}

#endif
