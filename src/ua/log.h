#ifndef UA_LOG_H
#define UA_LOG_H

#include <iostream>
#include <sstream>

/*
 * Trivial support for logging to stderr in a thread-safe manner (logging
 * is atomic at the level of a chain of insertion (<<) operations).
 *
 * Inspired by http://www.drdobbs.com/cpp/201804215.
 */

class Log {
public:

  Log() {
  }

  ~Log() {
    std::cerr << oss.str();
  }

  std::ostringstream& get() {
    return oss;
  }

private:

  std::ostringstream oss;
};

#define LOG Log().get()

#endif
