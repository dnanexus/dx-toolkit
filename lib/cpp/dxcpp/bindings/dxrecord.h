#ifndef DXCPP_BINDINGS_DXRECORD_H
#define DXCPP_BINDINGS_DXRECORD_H

#include "../bindings.h"

class DXRecord: public DXClass {
 public:
  dx::JSON describe() const { return jsonDescribe(dxid_); }
  dx::JSON getProperties(const dx::JSON &keys=dx::JSON()) const { return jsonGetProperties(dxid_, keys); }
  void setProperties(const dx::JSON &properties) const { jsonSetProperties(dxid_, properties); }
  void addTypes(const dx::JSON &types) const { jsonAddTypes(dxid_, types); }
  void removeTypes(const dx::JSON &types) const { jsonRemoveTypes(dxid_, types); }
  void destroy() { jsonDestroy(dxid_); }

  // Record-specific functions

  DXRecord() {}
  DXRecord(const std::string &dxid) { setID(dxid); }

  /**
   * Creates a new remote JSON object and initializes the value with
   * the given JSON.  The handler is updated with the object ID.
   *
   * @param to_store JSON to store in the remote JSON object.
   */
  void create(const dx::JSON &to_store);

  /**
   * Retrieves the stored JSON.
   *
   * @return The stored JSON of the remote JSON object.
   */
  dx::JSON get() const;

  /**
   * Sets the value of the remote JSON object with the given JSON.
   *
   * @param to_store JSON to store in the remote JSON object.
   */
  void set(const dx::JSON &to_store) const;

  /**
   * Given a JSON, create a new remote JSON object and initialize it
   * with the given JSON.
   *
   * @param to_store JSON to store in the remote JSON object.
   * @return A DXRecord remote object handler.
   */
  static DXRecord newDXRecord(const dx::JSON &to_store);
};

#endif
