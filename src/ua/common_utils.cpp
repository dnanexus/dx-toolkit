#include "common_utils.h"

#include <string>
#include "dxcpp/dxlog.h"

#if LINUX_BUILD

namespace LC_ALL_Hack {
  boost::mutex LC_ALL_Mutex;
  
  using namespace std;
  using namespace dx;

  pair<bool, string> originalValue; // first element is true iff LC_ALL was set to something previously, and second element of is the original value (is empty if first elem is "false")
  void set_LC_ALL_C() {
    boost::mutex::scoped_lock envLock(LC_ALL_Mutex);
    char *orig = getenv("LC_ALL"); // note: this pointer will be modified by subsequent calls to setenv/unsetenv
    DXLOG(logINFO) << "In set_LC_ALL_C() ...";
    if (orig != NULL) {
      originalValue = make_pair(true, string(orig));
      DXLOG(logINFO) << "env variable LC_ALL already present, value = '" << originalValue.second << "'";
    } else {
      originalValue = make_pair(false, "");
      DXLOG(logINFO) << "env variable LC_ALL is not previously set";
    }
    int ret_val = setenv("LC_ALL", "C", 1);
    DXLOG(logINFO) << "Setting env variable LC_ALL to 'C', return value = " << ret_val;
  }

  void reset_LC_ALL() {
    boost::mutex::scoped_lock envLock(LC_ALL_Mutex);
    DXLOG(logINFO) << "In reset_LC_ALL() ...";
    int ret_val;
    if (originalValue.first) {
      ret_val = setenv("LC_ALL", originalValue.second.c_str(), 1);
      DXLOG(logINFO) << "Setting env variable LC_ALL back to '" << originalValue.second << "', return value = " << ret_val;
    } else {
      ret_val = unsetenv("LC_ALL");
      DXLOG(logINFO) << "Unsetting env variable LC_ALL, return value = " << ret_val;
    }
  }
}

#endif
