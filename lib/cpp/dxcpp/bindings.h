#ifndef DXCPP_BINDINGS_H
#define DXCPP_BINDINGS_H

#include <string>
#include <limits>
#include "dxcpp.h"

extern std::string g_WORKSPACE_ID;

class DXDataObject {
 protected:
  std::string dxid_;
  std::string proj_;
  void waitOnState(const std::string &state="closed",
		   const int timeout=std::numeric_limits<int>::max()) const;

  virtual dx::JSON describe_(const std::string &input_params) const = 0;
  virtual void addTypes_(const std::string &input_params) const = 0;
  virtual void removeTypes_(const std::string &input_params) const = 0;
  virtual dx::JSON getDetails_(const std::string &input_params) const = 0;
  virtual void setDetails_(const std::string &input_params) const = 0;
  virtual void setVisibility_(const std::string &input_params) const = 0;
  virtual void rename_(const std::string &input_params) const = 0;
  virtual void setProperties_(const std::string &input_params) const = 0;
  virtual void addTags_(const std::string &input_params) const = 0;
  virtual void removeTags_(const std::string &input_params) const = 0;
  virtual void close_(const std::string &input_params) const = 0;
  virtual dx::JSON listProjects_(const std::string &input_params) const = 0;

 public:
  DXDataObject() { }
 DXDataObject(const std::string &dxid, const std::string &proj=g_WORKSPACE_ID) :
  dxid_(dxid), proj_(proj) { }

  std::string getID() const { return dxid_; }
  std::string getProjectID() const { return proj_; }
  virtual void setIDs(const std::string &dxid,
		      const std::string &proj="default");

  /** Returns a JSON object with, at minimum, the keys "id", "class",
   * "types", and "createdAt".  Other fields may also be included,
   * depending on the class.
   * @return JSON description
   */
  dx::JSON describe(bool incl_properties=false) const;
  void addTypes(const dx::JSON &types) const;
  void removeTypes(const dx::JSON &types) const;
  dx::JSON getDetails() const;
  void setDetails(const dx::JSON &details) const;
  void setVisibility(bool hidden) const;
  void rename(const std::string &name) const;
  void setProperties(const dx::JSON &properties) const;
  void addTags(const dx::JSON &tags) const;
  void removeTags(const dx::JSON &tags) const;
  void close() const;
  dx::JSON listProjects() const;
  void remove();
};

#include "bindings/dxrecord.h"
//#include "bindings/dxcollection.h"
#include "bindings/dxfile.h"
#include "bindings/dxgtable.h"
#include "bindings/dxprogram.h"
#include "bindings/dxjob.h"

#endif
