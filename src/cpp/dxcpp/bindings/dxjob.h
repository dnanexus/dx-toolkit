#ifndef DXCPP_BINDINGS_DXJOB_H
#define DXCPP_BINDINGS_DXJOB_H

#include <string>
#include <limits>
#include "dxjson/dxjson.h"

class DXJob {
 private:
  std::string dxid_;
 public:
  DXJob() { }
 DXJob(const std::string &dxid) : dxid_(dxid) { }

  dx::JSON describe() const;

  // Job-specific functions

  void setID(const std::string &dxid) { dxid_ = dxid; }
  std::string getID() const { return dxid_; }

  void create(const dx::JSON &fn_input, const std::string &fn_name);
  void terminate() const;
  void waitOnDone(const int timeout=std::numeric_limits<int>::max()) const;
};

#include "../bindings.h"

#endif
