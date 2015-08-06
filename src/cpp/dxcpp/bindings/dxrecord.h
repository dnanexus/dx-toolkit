// Copyright (C) 2013-2014 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

/** \file
 *
 * \brief Records.
 */

#ifndef DXCPP_BINDINGS_DXRECORD_H
#define DXCPP_BINDINGS_DXRECORD_H

#include "../bindings.h"

namespace dx {
  //! A minimal data object.

  ///
  /// A record stores no additional data, nor does it have any additional routes beyond those common
  /// to all data objects. A record object can store data in its details (see
  /// DXDataObject::setDetails and DXDataObject::getDetails) and can thereby act as an object
  /// containing metadata and links to other objects.
  ///

  class DXRecord: public DXDataObject {
   private:
    dx::JSON describe_(const std::string &s)const{return recordDescribe(dxid_,s);}
    void addTypes_(const std::string &s)const{recordAddTypes(dxid_,s);}
    void removeTypes_(const std::string &s)const{recordRemoveTypes(dxid_,s);}
    dx::JSON getDetails_(const std::string &s)const{return recordGetDetails(dxid_,s);}
    void setDetails_(const std::string &s)const{recordSetDetails(dxid_,s);}
    void setVisibility_(const std::string &s)const{recordSetVisibility(dxid_,s);}
    void rename_(const std::string &s)const{recordRename(dxid_,s);}
    void setProperties_(const std::string &s)const{recordSetProperties(dxid_,s);}
    void addTags_(const std::string &s)const{recordAddTags(dxid_,s);}
    void removeTags_(const std::string &s)const{recordRemoveTags(dxid_,s);}
    void close_(const std::string &s)const{recordClose(dxid_,s);}
    dx::JSON listProjects_(const std::string &s)const{return recordListProjects(dxid_,s);}

   public:
    // Record-specific functions

    DXRecord() { }
    
    /**
     * Creates a new %DXRecord handler for the specified remote record object.
     *
     * @param dxid Record ID.
     * @param proj ID of the project in which to access the object (if NULL, then default workspace will be used).
     */
    DXRecord(const char *dxid, const char *proj=NULL) {
      setIDs(std::string(dxid), (proj == NULL) ? config::CURRENT_PROJECT() : std::string(proj));
    }

    /**
     * Creates a new %DXRecord handler for the specified remote record object.
     *
     * @param dxid Record ID.
     * @param proj ID of the project in which to access the object.
     */
    DXRecord(const std::string &dxid,
             const std::string &proj=config::CURRENT_PROJECT()) { setIDs(dxid, proj); }

    /**
     * Creates a new %DXRecord handler for the specified remote record object.
     *
     * @param dxlink A JSON representing a <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Details-and-Links#Linking">DNAnexus link</a>.
     *  You may also use the extended form: {"$dnanexus_link": {"project": proj-id, "id": obj-id}}.
     */
    DXRecord(const dx::JSON &dxlink) { setIDs(dxlink); }

    /**
     * Creates a new remote record object. The handler is updated with the object ID.
     *
     * @param data_obj_fields JSON containing the optional fields with which to create the object
     * ("project", "types", "details", "hidden", "name", "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Records#API-method%3A-%2Frecord%2Fnew">/record/new</a>
     * API method.
     */
    void create(const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

    /**
     * Creates a new remote record object, initializing it from the specified record (and overriding
     * with any values that are present in data_obj_fields). The handler is updated with the object
     * ID.
     *
     * @param init_from a DXRecord from which to initialize the metadata
     * @param data_obj_fields JSON containing the optional fields with which to create the object
     * ("project", "types", "details", "hidden", "name", "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Records#API-method%3A-%2Frecord%2Fnew">/record/new</a>
     * API method.
     */
    void create(const DXRecord &init_from,
                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

    /**
     * Creates a new remote record. Returns a handler for the new object.
     *
     * @param data_obj_fields JSON containing the optional fields with which to create the object
     * ("project", "types", "details", "hidden", "name", "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Records#API-method%3A-%2Frecord%2Fnew">/record/new</a>
     * API method.
     *
     * @return A DXRecord remote object handler.
     */
    static DXRecord newDXRecord(const dx::JSON &data_obj_fields=
                                dx::JSON(dx::JSON_OBJECT));

    /**
     * Creates a new remote record object, initializing it from the specified record (and overriding
     * with any values that are present in data_obj_fields). Returns a handler for the new remote
     * object.
     *
     * @param init_from a DXRecord from which to initialize the metadata.
     * @param data_obj_fields JSON containing the optional fields with which to create the object
     * ("project", "types", "details", "hidden", "name", "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Records#API-method%3A-%2Frecord%2Fnew">/record/new</a>
     * API method.
     *
     * @return A DXRecord remote object handler.
     */
    static DXRecord newDXRecord(const DXRecord &init_from,
                                const dx::JSON &data_obj_fields=
                                dx::JSON(dx::JSON_OBJECT));

    /**
     * Clones the remote record into the specified project and folder.
     *
     * @param dest_proj_id ID of the project to which the object should be cloned.
     * @param dest_folder Folder route in which to put it in the destination project.
     *
     * @return New object handler with the associated project set to dest_proj_id.
     */
    DXRecord clone(const std::string &dest_proj_id,
                   const std::string &dest_folder="/") const;
  };
}
#endif
