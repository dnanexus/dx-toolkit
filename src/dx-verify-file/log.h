#ifndef UA_LOG_H
#define UA_LOG_H

#include <sstream>

/*
 * Trivial support for logging to stderr in a thread-safe manner (logging
 * is atomic at the level of a chain of insertion (<<) operations).
 *
 * Inspired by http://www.drdobbs.com/cpp/201804215.
 */

class Log {
public:

  Log();
  ~Log();
  std::ostringstream& get();

  static bool enabled;

private:

  std::ostringstream oss;
};

#define LOG Log().get()

#endif
