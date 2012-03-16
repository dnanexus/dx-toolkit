#ifndef DXCPP_BINDINGS_H
#define DXCPP_BINDINGS_H

#include<string>
#include "dxcpp.h"

using namespace std;

JSON search();
// TODO: Figure out signature
//classname=None, properties=None, typename=None, #permission=None,
//         describe=False

class DXClass {
 protected:
  string dxid_;
  void wait_on_state(const string &state="closed") const;

 public:
  string getID() const { return dxid_; }
  virtual void setID(const string &dxid) { dxid_ = dxid; }

  /** Returns a JSON object with, at minimum, the keys "id", "class",
   * "types", and "createdAt".  Other fields may also be included,
   * depending on the class.
   * @return JSON description
   */
  virtual JSON describe() const = 0;
  virtual JSON getProperties(const JSON &keys=JSON()) const = 0;
  virtual void setProperties(const JSON &properties) const = 0;
  virtual JSON getTypes() const { JSON desc = this->describe(); return desc["types"]; }
  virtual void addTypes(const JSON &types) const = 0;
  virtual void removeTypes(const JSON &types) const = 0;
  virtual void destroy() = 0;
};

#include "bindings/dxuser.h"
#include "bindings/dxgroup.h"
#include "bindings/dxjson.h"
#include "bindings/dxcollection.h"
#include "bindings/dxfile.h"
#include "bindings/dxtable.h"
#include "bindings/dxapp.h"
#include "bindings/dxjob.h"

#endif
