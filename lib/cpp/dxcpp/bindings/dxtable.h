#ifndef DXCPP_BINDINGS_DXTABLE_H
#define DXCPP_BINDINGS_DXTABLE_H

#include "../bindings.h"
#include "../api.h"

/**
 * @brief Remote table handler
 *
 */
class DXTable: public DXClass {
 private:
  dx::JSON row_buffer_;
  int row_buffer_maxsize_;
  int part_index_;

 public:
 DXTable() : DXClass(),
    row_buffer_(dx::JSON(dx::JSON_ARRAY)),
    row_buffer_maxsize_(104857600),
    part_index_(0) { }
 DXTable(const std::string & dxid) : DXClass(dxid),
    row_buffer_(dx::JSON(dx::JSON_ARRAY)),
    row_buffer_maxsize_(104857600),
    part_index_(0) { }

  /** Describes the object.
   * @see DXClass::describe()
   */
  dx::JSON describe() const { return tableDescribe(dxid_); }
  dx::JSON getProperties(const dx::JSON &keys) const { return tableGetProperties(dxid_, keys); }
  void setProperties(const dx::JSON &properties) const { tableSetProperties(dxid_, properties); }
  void addTypes(const dx::JSON &types) const { tableAddTypes(dxid_, types); }
  void removeTypes(const dx::JSON &types) const { tableRemoveTypes(dxid_, types); }
  void destroy() { tableDestroy(dxid_); }

  // Table-specific functions

  /**
   * 
   */
  void create(const dx::JSON &columns);
  void create(const dx::JSON &columns,
	      const std::string &chr_col,
	      const std::string &lo_col,
	      const std::string &hi_col);

  DXTable extend(const dx::JSON &columns) const;

  dx::JSON getRows(const dx::JSON &column_names=dx::JSON(dx::JSON_NULL),
		   const int starting=-1, const int limit=-1) const;
  dx::JSON getRows(const std::string &chr, const int lo, const int hi,
		   const dx::JSON &column_names=dx::JSON(dx::JSON_NULL),
		   const int starting=-1, const int limit=-1) const;
  void addRows(const dx::JSON &data, int index);
  void addRows(const dx::JSON &data); // For automatic index generation

  int getUnusedPartIndex();
  void flush();

  /**
   * Attempts to close the remote table.
   * @param block if true, waits until the table has finished closing before returning
   */
  void close(const bool block=false) ;

  /**
   * Waits until the remote table has finished closing.
   */
  void waitOnClose() const;

  static DXTable openDXTable(const std::string &dxid);

  static DXTable newDXTable(const dx::JSON &columns);

  static DXTable newDXTable(const dx::JSON &columns,
			    const std::string &chr_col,
			    const std::string &lo_col,
			    const std::string &hi_col);

  static DXTable extendDXTable(const std::string &dxid, const dx::JSON &columns);

  static dx::JSON columnDesc(const std::string &name,
			     const std::string &type);
};

#endif
