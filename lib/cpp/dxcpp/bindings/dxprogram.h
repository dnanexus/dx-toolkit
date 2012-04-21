#ifndef DXCPP_BINDINGS_DXPROGRAM_H
#define DXCPP_BINDINGS_DXPROGRAM_H

#include "../bindings.h"

class DXProgram: public DXDataObject {
 private:
  dx::JSON describe_(const std::string &s)const{return programDescribe(dxid_,s);}
  void addTypes_(const std::string &s)const{programAddTypes(dxid_,s);}
  void removeTypes_(const std::string &s)const{programRemoveTypes(dxid_,s);}
  dx::JSON getDetails_(const std::string &s)const{return programGetDetails(dxid_,s);}
  void setDetails_(const std::string &s)const{programSetDetails(dxid_,s);}
  void setVisibility_(const std::string &s)const{programSetVisibility(dxid_,s);}
  void rename_(const std::string &s)const{programRename(dxid_,s);}
  void setProperties_(const std::string &s)const{programSetProperties(dxid_,s);}
  void addTags_(const std::string &s)const{programAddTags(dxid_,s);}
  void removeTags_(const std::string &s)const{programRemoveTags(dxid_,s);}
  void close_(const std::string &s)const{programClose(dxid_,s);}
  dx::JSON listProjects_(const std::string &s)const{return programListProjects(dxid_,s);}

 public:
  // Program-specific functions
  DXProgram() { }
  DXProgram(const std::string &dxid,
            const std::string &proj=g_WORKSPACE_ID) { setIDs(dxid, proj); }

  void createFromFile(const std::string &codefile) const;
  void createFromString(const std::string &codestring) const;
  DXJob run(const dx::JSON &program_input,
            const std::string &project_context=g_WORKSPACE_ID,
            const std::string &output_folder="/") const;

  /**
   * Clones the associated object into the specified project and folder.
   *
   * @param dest_proj_id ID of the project to which the object should
   * be cloned
   * @param dest_folder Folder route in which to put it in the
   * destination project.
   * @return New object handler with the associated project set to
   * dest_proj_id.
   */
  DXProgram clone(const std::string &dest_proj_id,
                  const std::string &dest_folder="/") const;
};

#endif
