#ifndef DXCPP_BINDINGS_H
#define DXCPP_BINDINGS_H

#include <string>
#include <limits>
#include "dxcpp.h"

dx::JSON search();
// TODO: Figure out signature
//classname=None, properties=None, typename=None, #permission=None,
//         describe=False

class DXClass {
 protected:
  std::string dxid_;
  void waitOnState(const std::string &state="closed",
		   const int timeout=std::numeric_limits<int>::max()) const;

 public:
  DXClass() { }
 DXClass(const std::string &dxid) : dxid_(dxid) { }

  std::string getID() const { return dxid_; }
  virtual void setID(const std::string &dxid) { dxid_ = dxid; }

  /** Returns a JSON object with, at minimum, the keys "id", "class",
   * "types", and "createdAt".  Other fields may also be included,
   * depending on the class.
   * @return JSON description
   */
  virtual dx::JSON describe() const = 0;
  virtual dx::JSON getProperties(const dx::JSON &keys=dx::JSON()) const = 0;
  virtual void setProperties(const dx::JSON &properties) const = 0;
  virtual dx::JSON getTypes() const { dx::JSON desc = describe(); return desc["types"]; }
  virtual void addTypes(const dx::JSON &types) const = 0;
  virtual void removeTypes(const dx::JSON &types) const = 0;
  virtual void destroy() = 0;
};

#include "bindings/dxuser.h"
#include "bindings/dxgroup.h"
#include "bindings/dxrecord.h"
#include "bindings/dxcollection.h"
#include "bindings/dxfile.h"
#include "bindings/dxtable.h"
#include "bindings/dxapp.h"
#include "bindings/dxjob.h"

#endif
