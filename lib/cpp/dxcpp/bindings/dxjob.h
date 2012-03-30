#ifndef DXCPP_BINDINGS_DXJOB_H
#define DXCPP_BINDINGS_DXJOB_H

#include "../bindings.h"

class DXJob: public DXClass {
 public:
  dx::JSON describe() const { return jobDescribe(dxid_); }
  dx::JSON getProperties(const dx::JSON &keys) const { return jobGetProperties(dxid_, keys); }
  void setProperties(const dx::JSON &properties) const { jobSetProperties(dxid_, properties); }
  void addTypes(const dx::JSON &types) const { jobAddTypes(dxid_, types); }
  void removeTypes(const dx::JSON &types) const { jobRemoveTypes(dxid_, types); }
  void destroy() { jobDestroy(dxid_); }

  // Job-specific functions

  void create(const dx::JSON &fn_input, const std::string &fn_name);
  void waitOnDone() const;
};

#endif
