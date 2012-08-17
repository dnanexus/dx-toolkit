#ifndef DXCPP_BINDINGS_DXAPPLET_H
#define DXCPP_BINDINGS_DXAPPLET_H

#include "../bindings.h"

class DXApplet: public DXDataObject {
private:
  dx::JSON describe_(const std::string &s)const{return appletDescribe(dxid_,s);}
  void addTypes_(const std::string &s)const{appletAddTypes(dxid_,s);}
  void removeTypes_(const std::string &s)const{appletRemoveTypes(dxid_,s);}
  dx::JSON getDetails_(const std::string &s)const{return appletGetDetails(dxid_,s);}
  void setDetails_(const std::string &s)const{appletSetDetails(dxid_,s);}
  void setVisibility_(const std::string &s)const{appletSetVisibility(dxid_,s);}
  void rename_(const std::string &s)const{appletRename(dxid_,s);}
  void setProperties_(const std::string &s)const{appletSetProperties(dxid_,s);}
  void addTags_(const std::string &s)const{appletAddTags(dxid_,s);}
  void removeTags_(const std::string &s)const{appletRemoveTags(dxid_,s);}
  void close_(const std::string &s)const{appletClose(dxid_,s);}
  dx::JSON listProjects_(const std::string &s)const{return appletListProjects(dxid_,s);}

public:
  // Note: We do not provide applet creation function .. since we want users
  // to use applet_builder for that task.

  // Applet-specific functions
  DXApplet() { }
  
  DXApplet(const std::string &dxid,
            const std::string &proj=g_WORKSPACE_ID) { setIDs(dxid, proj); }
  
  /**
   * Create a new applet with specified input hash.
   * For details about input see the route: /applet-xxx/new.
   *
   * If inp["project"] is missing, then g_WORKSPACE_ID is used as the
   * inp["project"]
   *
   * @param inp JSON hash, which will be passed (after resolving missing project)
   * to route: /applet-xxxx/new
   */
  void create(dx::JSON inp);

  /** 
   * Creates a new job, to execute the function "main" of this applet
   * with the given input.
   *
   * @param applet_input JSON Hash of the applet's input argument
   * @param project_context Project context in which the applet would be run
   * @param output_folder Folder inside current project (decided by context)
   * where applet's output would be placed.
   *
   * @return Handler for the Job launched.
   */
  DXJob run(const dx::JSON &applet_input,
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
  DXApplet clone(const std::string &dest_proj_id,
                  const std::string &dest_folder="/") const;

  /**
   * Returns the full specification of the applet as a JSON object.
   * See route: /applet-xxxx/get for details.
   *
   * @return Full specification of the applet
   */
  dx::JSON get() const { return appletGet(dxid_); }

};

#endif
