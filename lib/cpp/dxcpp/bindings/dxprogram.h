#ifndef DXCPP_BINDINGS_DXPROGRAM_H
#define DXCPP_BINDINGS_DXPROGRAM_H

#include "../bindings.h"

class DXProgram: public DXDataObject {
 private:
  dx::JSON describe_(const std::string &input_params) const {
    return programDescribe(dxid_, input_params);
  }
  void addTypes_(const std::string &input_params) const {
    programAddTypes(dxid_, input_params);
  }
  void removeTypes_(const std::string &input_params) const {
    programRemoveTypes(dxid_, input_params);
  }
  dx::JSON getDetails_(const std::string &input_params) const {
    return programGetDetails(dxid_, input_params);
  }
  void setDetails_(const std::string &input_params) const {
    programSetDetails(dxid_, input_params);
  }
  void setVisibility_(const std::string &input_params) const {
    programSetVisibility(dxid_, input_params);
  }
  void rename_(const std::string &input_params) const {
    programRename(dxid_, input_params);
  }
  void setProperties_(const std::string &input_params) const {
    programSetProperties(dxid_, input_params);
  }
  void addTags_(const std::string &input_params) const {
    programAddTags(dxid_, input_params);
  }
  void removeTags_(const std::string &input_params) const {
    programRemoveTags(dxid_, input_params);
  }
  void close_(const std::string &input_params) const {
    programClose(dxid_, input_params);
  }
  dx::JSON listProjects_(const std::string &input_params) const {
    return programListProjects(dxid_, input_params);
  }

 public:
  // App-specific functions

  void createFromFile(const std::string &codefile) const;
  void createFromString(const std::string &codestring) const;
  void run() const;
};

#endif
