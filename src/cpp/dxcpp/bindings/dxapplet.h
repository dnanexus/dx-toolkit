/** \file
 *
 * \brief Applets.
 */

//! An executable object representing an analysis or other piece of software.

///
/// An applet operates on input data and produces output data. Both the inputs and the outputs of
/// an applet can include a combination of simple objects (of numeric, string, hash, or boolean
/// type; passed by value) or data objects (passed by reference).
///
/// To publish your software for public consumption, create an App object (represented by DXApp)
/// instead.
///
/// To create a new applet object, consider using the <code>dx-build-applet</code> command-line
/// tool in the DNAnexus SDK.
///
/// See <a href="http://wiki.dev.dnanexus.com/API-Specification-v1.0.0/Applets">Applets</a> in the
/// API specification for more information.
///

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

  /**
   * Creates a %DXApplet handler for the specified remote applet.
   *
   * @param dxid applet ID
   * @param proj ID of the project in which the applet should be accessed
   */
  DXApplet(const std::string &dxid,
           const std::string &proj=g_WORKSPACE_ID) { setIDs(dxid, proj); }

  /**
   * Creates a new remote applet with the input hash, as specified in the <a
   * href="http://wiki.dev.dnanexus.com/API-Specification-v1.0.0/Applets#API-method%3A-%2Fapplet%2Fnew">/applet/new</a>
   * API method.
   *
   * If <code>inp["project"]</code> is missing, then <code>g_WORKSPACE_ID</code> will be used as
   * the destination project.
   *
   * @param inp JSON hash representing the applet to be created
   */
  void create(dx::JSON inp);

  /**
   * Runs this applet with the specified input and returns a handler for the resulting job.
   *
   * See the <a
   * href="http://wiki.dev.dnanexus.com/API-Specification-v1.0.0/Applets#API-method%3A-%2Fapplet-xxxx%2Frun">/applet-xxxx/run</a>
   * API method for more info.
   *
   * @param applet_input A hash of name/value pairs specifying the input that the app is to be launched with.
   * @param project_context A string representing the project context in which the applet is to be run.
   * @param output_folder The folder (within the project_context) in which the applet's output objects will be placed.
   *
   * @return Handler for the job that was launched.
   */
  DXJob run(const dx::JSON &applet_input,
            const std::string &project_context=g_WORKSPACE_ID,
            const std::string &output_folder="/") const;

  /**
   * Clones the applet into the specified project and folder.
   *
   * @param dest_proj_id ID of the project to which the object should be cloned
   * @param dest_folder Folder route in destination project into which the clone should be placed.
   *
   * @return New object handler with the associated project set to dest_proj_id.
   */
  DXApplet clone(const std::string &dest_proj_id,
                 const std::string &dest_folder="/") const;

  /**
   * Returns the full specification of the applet, as specified in the <a
   * href="http://wiki.dev.dnanexus.com/API-Specification-v1.0.0/Applets#API-method%3A-%2Fapplet-xxxx%2Fget">/applet-xxxx/get</a>
   * API method.
   *
   * @return JSON hash containing the full specification of the applet
   */
  dx::JSON get() const { return appletGet(dxid_); }

};

#endif
