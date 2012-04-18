#ifndef DXCPP_BINDINGS_DXJOB_H
#define DXCPP_BINDINGS_DXJOB_H

#include "../bindings.h"

class DXJob {
 private:
  std::string dxid_;
 public:
  DXJob() { }
 DXJob(const std::string &dxid) : dxid_(dxid) { }

  dx::JSON describe() const { return jobDescribe(dxid_); }

  // Job-specific functions

  void create(const dx::JSON &fn_input, const std::string &fn_name);
  void waitOnDone(const int timeout=std::numeric_limits<int>::max()) const;
};

#endif
