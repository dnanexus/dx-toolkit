#ifndef DXCPP_BINDINGS_DXJOB_H
#define DXCPP_BINDINGS_DXJOB_H

#include "../bindings.h"

class DXJob: public DXClass {
 public:
  JSON describe() const { return jobDescribe(dxid_); }
  JSON getProperties(const JSON &keys) const { return jobGetProperties(dxid_, keys); }
  void setProperties(const JSON &properties) const { jobSetProperties(dxid_, properties); }
  void addTypes(const JSON &types) const { jobAddTypes(dxid_, types); }
  void removeTypes(const JSON &types) const { jobRemoveTypes(dxid_, types); }
  void destroy() { jobDestroy(dxid_); }

  // Job-specific functions

  void create(const JSON &fn_input, const string &fn_name);
  void wait_on_done() const;
};

#endif
