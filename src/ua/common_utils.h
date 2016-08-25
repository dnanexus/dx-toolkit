#ifndef UA_COMMON_UTILS_H
#define UA_COMMON_UTILS_H

#include <boost/thread.hpp>

#if LINUX_BUILD
namespace LC_ALL_Hack {
  // Note: It's user's responsibility to ensure that multiple calls set_LC_ALL_C() are not made
  //       before calling reset_LC_ALL() in between. We provide a global LC_ALL_Mutex variable to help user implement it.

  extern boost::mutex LC_ALL_Mutex;
  void set_LC_ALL_C();
  void reset_LC_ALL();
}
#endif

#endif
