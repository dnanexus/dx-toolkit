#ifndef DXCPP_BINDINGS_H
#define DXCPP_BINDINGS_H

#import<string>
#import "dxcpp.h"
#import "api.h"
#import "json.h"

namespace dxpy {

  using namespace std;

  JSON search();
  // TODO: Figure out signature
  //classname=None, properties=None, typename=None, #permission=None,
  //         describe=False

  class DXClass {
  protected:
    string dxid;

  public:
    DXClass() {}
  DXClass(string dxid_) : dxid(dxid_) {}

    string getID() { return dxid; }
    virtual void setID(string dxid_) { dxid = dxid_; }

    virtual JSON describe() =0;
    virtual JSON getProperties() =0;
    virtual void setProperties() =0;
    virtual void addTypes() =0;
    virtual void removeTypes() =0;
    virtual void destroy() =0;
  };
}

#import "bindings/dxuser.h"
#import "bindings/dxgroup.h"
#import "bindings/dxjson.h"
#import "bindings/dxcollection.h"
#import "bindings/dxfile.h"
#import "bindings/dxtable.h"
#import "bindings/dxapp.h"
#import "bindings/dxjob.h"

#endif
