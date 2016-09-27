// Copyright (C) 2013-2016 DNAnexus, Inc.
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

#include "dxlog.h"
#include <boost/thread.hpp>
#include <boost/date_time.hpp>
#include <iostream>
#include <cstdlib>

using namespace std;

namespace dx {
  boost::mutex Log::mtx;
  Log::~Log() {
    try {
      boost::mutex::scoped_lock lock(mtx);
      cerr << oss.str() << endl;
    }
    catch(...) {
      cerr << oss.str() << endl;
    }
  }

  static int64_t NowTime() {
    static boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
    return (boost::posix_time::microsec_clock::universal_time() - epoch).total_milliseconds();
  }

  std::string Log::ToString(LogLevel level) {
    switch (level) {
      case logERROR: return "ERROR";
      case logWARNING: return "WARNING";
      case logINFO: return "INFO";
      case logDEBUG: return "DEBUG";
      case logDEBUG1: return "DEBUG1";
      case logDEBUG2: return "DEBUG2";
      case logDEBUG3: return "DEBUG3";
      case logDEBUG4: return "DEBUG4";
      case logUSERINFO: return "USERINFO";
    }
    return "UNKNOWN_LOG_LEVEL";
  }

  ostringstream& Log::Get(LogLevel level) {
    if (level != logUSERINFO) {
      oss << "[" << NowTime();
      oss << " " << boost::this_thread::get_id();
      oss << "] " << Log::ToString(level) << ": ";
      oss << string((level >= logDEBUG) ? 0: logDEBUG - level, '\t'); // indentation for higher level debug messages
    }
    return oss;
  }

  LogLevel& Log::ReportingLevel() {
    static LogLevel val;
    return val;
  }
 
  void Log::Init() {
    #if DEBUG
      Log::ReportingLevel() = logDEBUG4; // report everything in debug mode by default
      return;
    #endif
    const bool debug_mode_from_env = (getenv("DXCPP_DEBUG") != NULL); // If environment variable DXCPP_DEBUG is set, then set logging level to DEBUG4
    Log::ReportingLevel() = (debug_mode_from_env) ? logDEBUG4 : logWARNING;
  }
}
