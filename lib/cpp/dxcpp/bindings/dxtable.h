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
   * Sets the remote object ID associated with the remote table
   * handler.  If the handler had rows queued up in the internal
   * buffer, they are flushed.
   *
   * @param dxid Remote object ID of the remote table to be accessed
   */
  void setID(const std::string &dxid);

  /**
   * 
   */
  void create(const dx::JSON &columns);
  void create(const dx::JSON &columns,
	      const std::string &chr_col,
	      const std::string &lo_col,
	      const std::string &hi_col);

  DXTable extend(const dx::JSON &columns) const;

  /**
   * Retrieves the requested rows and columns.
   *
   * @param column_names A JSON array listing the column names to be
   * returned; the order of the column names will be respected in the
   * output.  (Use the JSON null value to indicate all columns.)
   * @param starting An integer representing the first row id to
   * report.
   * @param limit An integer representing the limit on the number of
   * rows to be returned.
   * @return A JSON object with keys "size", "next", and "data".
   */
  dx::JSON getRows(const dx::JSON &column_names=dx::JSON(dx::JSON_NULL),
		   const int starting=-1, const int limit=-1) const;

  /**
   * R
   */
  dx::JSON getRows(const std::string &chr, const int lo, const int hi,
		   const dx::JSON &column_names=dx::JSON(dx::JSON_NULL),
		   const int starting=-1, const int limit=-1) const;

  /**
   * Adds the rows listed in data to the current table using the given
   * index as the part index.
   *
   * @param data A JSON array of row data (each row represented as
   * JSON arrays).
   * @param index An integer representing the part that the given rows
   * should be sent as.
   */
  void addRows(const dx::JSON &data, int index);

  /**
   * Adds the rows listed in data to the current table.  Rows will be
   * added to an internal buffer and will be flushed to the remote
   * server periodically using automatically generated part index
   * numbers.
   *
   * @param data A JSON array of row data (each row represented as JSON arrays).
   */
  void addRows(const dx::JSON &data); // For automatic index generation

  /**
   * Queries the remote table and finds a valid unused number (part
   * index) which can then be used to add rows to the remote table.
   * Regardless of the state of the remote table, the method will not
   * return the same part index more than once, i.e. requesting an
   * unused part index automatically increments the next search.
   *
   * @return An integer that has not yet been used to upload
   * rows to the remote table object
   */
  int getUnusedPartIndex();

  /**
   * Pushes rows stored in the internal buffer to the remote table.
   */
  void flush();

  /**
   * Attempts to close the remote table.
   *
   * @param block If true, waits until the table has finished closing
   * before returning.  Otherwise, it returns immediately.
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

  /**
   * Constructs a column descriptor from a column name and data type.
   * @param name Name of the column
   * @param type Data type to be stored in the column
   * @return A JSON object containing the column descriptor
   */
  static dx::JSON columnDesc(const std::string &name,
			     const std::string &type);
};

#endif
