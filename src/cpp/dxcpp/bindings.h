#ifndef DXCPP_BINDINGS_H
#define DXCPP_BINDINGS_H

#include <string>
#include <limits>
#include "dxcpp.h"

extern std::string g_WORKSPACE_ID;

/**
 * The DXDataObject class is the abstract base class for all data
 * object remote handlers.  It contains the common methods to all
 * handlers, such as creation, describing, setting of properties,
 * tags, types, visibility, details, etc.  Each DXDataObject instance
 * has two IDs associated with it: a data object ID assigned to the
 * data object it is accessing, and a project ID to indicate which
 * project's copy of the data the handler will be accessing.  Note
 * that it is possible to have two handlers with the same data object
 * ID but different project IDs.  Both will access the same underlying
 * data but can separately modify the name, properties, and tags.
 */
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

  /**
   * Clones the associated object into the specified project and folder.
   *
   * @param dest_proj_id ID of the project to which the object should
   * be cloned
   * @param dest_folder Folder route in which to put it in the
   * destination project.
   */
  void clone_(const std::string &dest_proj_id,
              const std::string &dest_folder) const;

 public:
  DXDataObject() { }
  DXDataObject(const DXDataObject &to_copy) {
    dxid_ = to_copy.dxid_;
    proj_ = to_copy.proj_;
  }
  DXDataObject(const std::string &dxid) { setIDs(dxid); }
  DXDataObject(const std::string &dxid, const std::string &proj) {
    setIDs(dxid, proj);
  }

  /**
   * @return ID of the associated data object
   */
  std::string getID() const { return dxid_; }

  /**
   * Default conversion to string is to its object ID so a handler can
   * always be passed in place of a string argument that expects an
   * object ID.
   */
  operator std::string() { return dxid_; }

  /**
   * @return ID of the project to which this data object handler is
   * accessing.
   */
  std::string getProjectID() const { return proj_; }

  /**
   * Sets the object and project IDs as specified.  The default value
   * for the project ID will be set according to the default
   * workspace.  See setWorkspaceID() and loadFromEnvironment() for
   * more information.
   *
   * @param dxid ID of the associated data object
   * @param proj ID of the project whose copy of the data object
   * should be accessed
   */
  virtual void setIDs(const std::string &dxid,
		      const std::string &proj=g_WORKSPACE_ID);

  /**
   * Returns a JSON object with, at minimum, the keys "id", "class",
   * "types", and "createdAt".  Other fields may also be included,
   * depending on the class.
   * @return JSON description
   */
  dx::JSON describe(bool incl_properties=false) const;

  /**
   * Adds the specified types.
   *
   * @param types JSON array of strings to add as types
   */
  void addTypes(const dx::JSON &types) const;

  /**
   * Removes the specified types.
   *
   * @param types JSON array of strings to remove as types
   */
  void removeTypes(const dx::JSON &types) const;

  /**
   * Retrieves the details stored in the object.
   *
   * @return JSON containing the remote object's details
   */
  dx::JSON getDetails() const;

  /**
   * Stores the given JSON in the details of the remote object.
   *
   * @param details Arbitrary JSON to store as details
   */
  void setDetails(const dx::JSON &details) const;

  /**
   * Ensures the remote object is hidden.
   */
  void hide() const;

  /**
   * Ensures the remote object is visible.
   */
  void unhide() const;

  /**
   * Renames the object.
   *
   * @param name New name for the object
   */
  void rename(const std::string &name) const;

  /**
   * Sets the specified properties.
   *
   * @param properties JSON OBJECT mapping strings to strings to store
   * as properties
   */
  void setProperties(const dx::JSON &properties) const;

  /**
   * Adds the specified tags.
   *
   * @param tags JSON array of strings to add as tags
   */
  void addTags(const dx::JSON &tags) const;

  /**
   * Removes the specified tags.
   *
   * @param tags JSON array of strings to remove as tags
   */
  void removeTags(const dx::JSON &tags) const;

  /**
   * Closes the object.
   */
  virtual void close() const;

  /**
   * Lists all projects which contain a copy of the object.
   *
   * @return JSON array of project IDs (strings) which contain a copy
   * of the object
   */
  dx::JSON listProjects() const;

  /**
   * Moves the associated object into the specified folder in the same
   * project.
   *
   * @param dest_folder Folder route in which to put the object
   */
  void move(const std::string &dest_folder) const;

  /**
   * Removes the copy of the object from the associated project (see
   * getProjectID()).  Does not affect copies of the object in other
   * projects.
   */
  void remove();
};

/**
 * Creates a JSON object that is a special DNAnexus link to an
 * existing data object ID.
 *
 * @param dxid Data object ID to link to
 * @param proj Project ID to specify in the link
 */
dx::JSON DXLink(const std::string &dxid, const std::string &proj="");

#include "bindings/dxrecord.h"
//#include "bindings/dxtable.h"
#include "bindings/dxfile.h"
#include "bindings/dxgtable.h"
#include "bindings/dxjob.h"
#include "bindings/dxapplet.h"
#include "bindings/dxapp.h"
#include "bindings/dxproject.h"

#endif
