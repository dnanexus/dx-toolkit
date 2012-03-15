#ifndef DXCPP_BINDINGS_DXJOB_H
#define DXCPP_BINDINGS_DXJOB_H

#import "bindings.h"

namespace dxpy {
  using namespace dxpy;

  class DXJob: public DXClass {
  public:
    JSON describe() { return jobDescribe(dxid); }
    JSON getProperties() { return jobGetProperties(dxid); }
    void setProperties() { jobSetProperties(dxid); }
    void addTypes() { jobAddTypes(dxid); }
    void removeTypes() { jobRemoveTypes(dxid); }
    void destroy() { jobDestroy(dxid); }

    // Job-specific functions

    void create(JSON fn_input, string fn_name);
    void wait_on_done();
  };
}

#endif
