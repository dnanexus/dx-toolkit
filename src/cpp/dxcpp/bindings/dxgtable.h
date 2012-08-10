#ifndef DXCPP_BINDINGS_DXGTABLE_H
#define DXCPP_BINDINGS_DXGTABLE_H

#include <sstream>
#include <boost/thread.hpp>
#include "../bindings.h"
#include "../api.h"
#include "../bqueue.h"
#include <boost/container/vector.hpp>
/**
 * @brief Remote table handler
 *
 */
class DXGTable: public DXDataObject {
private:
  dx::JSON describe_(const std::string &s)const{return gtableDescribe(dxid_,s);}
  void addTypes_(const std::string &s)const{gtableAddTypes(dxid_,s);}
  void removeTypes_(const std::string &s)const{gtableRemoveTypes(dxid_,s);}
  dx::JSON getDetails_(const std::string &s)const{return gtableGetDetails(dxid_,s);}
  void setDetails_(const std::string &s)const{gtableSetDetails(dxid_,s);}
  void setVisibility_(const std::string &s)const{gtableSetVisibility(dxid_,s);}
  void rename_(const std::string &s)const{gtableRename(dxid_,s);}
  void setProperties_(const std::string &s)const{gtableSetProperties(dxid_,s);}
  void addTags_(const std::string &s)const{gtableAddTags(dxid_,s);}
  void removeTags_(const std::string &s)const{gtableRemoveTags(dxid_,s);}
  void close_(const std::string &s)const{gtableClose(dxid_,s);}
  dx::JSON listProjects_(const std::string &s)const{return gtableListProjects(dxid_,s);}

  // Added for multi-threading in addRows
  void finalizeRequestBuffer_();
  void joinAllWriteThreads_();
  void writeChunk_(std::string);
  void createWriteThreads_();
  ///////////////////////////////////////
  
  // For get rows
  void readChunk_() const;
  /////////////////////////////////////
  
  std::stringstream row_buffer_;

  int64_t row_buffer_maxsize_;

  void reset_buffer_();
 
  // To allow interleaving (without compiler optimization possibly changing order)
  // we use std::atomic (a c++11 feature)
  // Ref https://parasol.tamu.edu/bjarnefest/program/boehm-slides.pdf (page 7) 
  // Update: Since not all compilers support atomic yet
  //         we use volatile and locking to ensure atomicity
 
  boost::mutex countThreadsMutex;
  volatile int countThreadsWaitingOnConsume, countThreadsNotWaitingOnConsume;
  // we use boost::container::vector, because it supports move semantics
  boost::container::vector<boost::thread> writeThreads;
  static const int MAX_WRITE_THREADS = 5;
  BlockingQueue<std::string> addRowRequestsQueue;
  
  // For linear query
  
  mutable std::map<int64_t, dx::JSON> lq_results_;
  mutable dx::JSON lq_columns_;
  mutable int64_t lq_chunk_limit_;
  mutable int64_t lq_query_start_;
  mutable int64_t lq_query_end_;
  mutable unsigned lq_max_chunks_;
  mutable int64_t lq_next_result_;
  mutable boost::container::vector<boost::thread> lq_readThreads_;
  mutable boost::mutex lq_results_mutex_, lq_query_start_mutex_;

public:

  DXGTable()
    : DXDataObject(), row_buffer_maxsize_(104857600), countThreadsWaitingOnConsume(0), countThreadsNotWaitingOnConsume(0)
  {
    reset_buffer_();
  }

  DXGTable(const DXGTable &to_copy)
    : DXDataObject(to_copy), row_buffer_maxsize_(104857600), countThreadsWaitingOnConsume(0), countThreadsNotWaitingOnConsume(0)
  {
    reset_buffer_(); setIDs(to_copy.dxid_, to_copy.proj_);
  }

  DXGTable(const std::string & dxid, const std::string &proj=g_WORKSPACE_ID)
    : row_buffer_maxsize_(104857600), countThreadsWaitingOnConsume(0), countThreadsNotWaitingOnConsume(0)
  {
    reset_buffer_(); setIDs(dxid, proj);
  }

  DXGTable& operator=(const DXGTable& to_copy) {
    if (this == &to_copy)
      return *this;

    this->row_buffer_maxsize_ = 104857600;
    this->reset_buffer_();
    this->setIDs(to_copy.dxid_, to_copy.proj_);
    return *this;
  }

  /**
   * Returns the buffer size (in bytes of the stringified rows) that
   * must be reached before rows are automatically flushed.
   *
   * @returns Buffer size
   */
  int64_t getMaxBufferSize() const {
    return row_buffer_maxsize_;
  }

  /**
   * Sets the buffer size (in bytes of the stringified rows) that must
   * be reached before rows are flushed.
   *
   * @param buf_size New buffer size to use
   */
  void setMaxBufferSize(const int64_t buf_size) {
    row_buffer_maxsize_ = buf_size;
  }

  // Table-specific functions

  /**
   * Sets the remote object ID associated with the remote table
   * handler.  If the handler had rows queued up in the internal
   * buffer, they are flushed.
   *
   * @param dxid Remote object ID of the remote table to be accessed
   * @param proj Project ID of the remote table to be accessed.
   */
  void setIDs(const std::string &dxid, const std::string &proj="default");

  /**
   * Creates a new GTable.
   *
   * @param columns Vector of column descriptors; must be nonempty
   * @param indices Vector of index descriptors
   * @param data_obj_fields JSON containing the optional fields with
   * which to create the object ("project", "types", "details",
   * "hidden", "name", "properties", "tags")
   */
  void create(const std::vector<dx::JSON> &columns,
              const std::vector<dx::JSON> &indices,
              const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  /**
   * Creates a new GTable.
   *
   * @param columns Vector of column descriptors; must be nonempty
   * @param data_obj_fields JSON containing the optional fields with
   * which to create the object ("project" if not using the default
   * project, "types", "details", "hidden", "name", "properties",
   * "tags")
   */
  void create(const std::vector<dx::JSON> &columns,
              const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT)) {
    create(columns, std::vector<dx::JSON>(), data_obj_fields);
  }

  /**
   * Creates a new GTable and initializes it using the metadata from
   * an existing GTable.  Note that the default behavior of creating a
   * new data object in the current workspace is still in effect and
   * needs to be explicitly stated if the project of the object
   * specified as init_from is to be used.
   *
   * @param init_from a GTable from which to initialize all metadata,
   * including column and index specs.
   * @param data_obj_fields
   */
  void create(const DXGTable &init_from,
	      const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  /**
   * Creates a new GTable.
   *
   * @param init_from a GTable from which to initialize all metadata,
   * including column and index specs.
   * @param columns Vector of column descriptors with which to
   * override the defaults specified by init_from.  This should be a
   * nonempty.
   * @param data_obj_fields
   */
  void create(const DXGTable &init_from,
	      const std::vector<dx::JSON> &columns,
	      const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  /**
   * Creates a new GTable.
   *
   * @param init_from a GTable from which to initialize all metadata,
   * including column and index specs.
   * @param columns Vector of column descriptors with which to
   * override the defaults specified by init_from.  Use an empty array
   * to avoid overriding it but to override the index specs.
   * @param indices Vector of index descriptors with which to override
   * the defaults specified in init_from.  Unlike columns in this
   * method, an empty array will explicitly set the index specs to be
   * an empty list.
   * @param data_obj_fields
   */
  void create(const DXGTable &init_from,
	      const std::vector<dx::JSON> &columns,
              const std::vector<dx::JSON> &indices,
	      const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  DXGTable extend(const std::vector<dx::JSON> &columns,
                  const std::vector<dx::JSON> &indices,
                  const dx::JSON &data_obj_fields=
                  dx::JSON(dx::JSON_OBJECT)) const;
  DXGTable extend(const std::vector<dx::JSON> &columns,
                  const dx::JSON &data_obj_fields=
                  dx::JSON(dx::JSON_OBJECT)) const {
    return extend(columns, std::vector<dx::JSON>(), data_obj_fields);
  }

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
   * @return A JSON object with keys "length", "next", and "data".
   */
  dx::JSON getRows(const dx::JSON &query=dx::JSON(dx::JSON_NULL),
                   const dx::JSON &column_names=dx::JSON(dx::JSON_NULL),
                   const int64_t starting=-1, const int64_t limit=-1) const;

  
  /** 
   * Start fetching rows in chunks of specified size from the gtable in background.
   * After calling this function, getNextChunk() can be use to access chunks in a
   * linear manner.
   * 
   * @note - Calling this function, invalidates any previous call to the function.
   * @param column_names A JSON array listing the column names to be
   * returned; the order of the column names will be respected in the
   * output.  (Use the JSON null value to indicate all columns.)
   * @param start_row Row number (0-indexed) starting from which
   * rows will be fetched.
   * @param num_rows Number of rows to be fetched
   * @param chunk_size Number of rows to be fetched in each chunk
   * (except possibly the last one, which can be shorter)
   * @param max_chunks An indicative number for chunks to be kept in memory
   * at any time. Note number of real chunks in memory would be < (max_chunks + thread_count)
   * @param thread_count Number of threads to be used for fetching rows.
   * @see stopLinearQuery(), getNextChunk()
   */
  void startLinearQuery(const dx::JSON &column_names=dx::JSON(dx::JSON_NULL),
                        const int64_t start_row=-1,
                        const int64_t num_rows=-1,
                        const int64_t chunk_size=10000,
                        const unsigned max_chunks=20,
                        const unsigned thread_count=5) const;
  
  /**
   * All fetching of chunks in background is stopped, and read threads terminated.
   * - Invalidates previous call to startLinearQuery() (if any).
   * - Idempotent.
   * @see startLinearQuery(), getNextChunk()
   */
  void stopLinearQuery() const;
  
  /**
   * This function is used after calling startLinearQuery() to get next row chunk
   * in order. If startLinearQuery() was not called, then it returns "false"
   * 
   * @param chunk If function returns with "true", then this object will be populated
   * with rows from next chunk (an array of arrays). If "false" is returned, then
   * this object remain untouched.
   *
   * @return "true" if another chunk is available for processing (value of chunk is
   * copied to object passed in as input param "chunk"). "false" if all chunks
   * have exhausted, or no call to startLinearQuery() was made.
   * @see startLinearQuery(), stopLinearQuery()
   */
  bool getNextChunk(dx::JSON &chunk) const;


  /**
   * Adds the rows listed in data to the current table using the given
   * number as the part ID.
   *
   * @note This function works quite differently from it's overloaded counterpart
   * addRows(const dx::JSON&).
   * - It is not multi threaded, and do not use any internal buffer.
   * - It is always blocking, and returns only after http request finishes.
   * @warning In general you should never mix and match between calls to
   * addRows(const dx::JSON&, int) and addRows(const dx::JSON&)
   * @param data A JSON array of row data (each row represented as
   * JSON arrays).
   * @param part_id An integer representing the part that the given
   * rows should be sent as.
   */
  void addRows(const dx::JSON &data, int part_id);

  /**
   * Adds the rows listed in data to the current table.  Rows will be
   * added to an internal buffer and will be flushed to the remote
   * server periodically using automatically generated part ID
   * numbers.
   *
   * For increasing throughput, this function uses multiple threads 
   * for adding rows in background. It will block only if MAX_WRITE_THREADS 
   * number of threads (i.e., all the workers) are already busy completing 
   * previous HTTP request(s), else it will pass on the task to one of 
   * the free worker thread and return.
   *
    * If any of the thread fails then std::terminate() would be called.
   * @warning In general you should never mix and match between calls to
   * addRows(const dx::JSON&, int) and addRows(const dx::JSON&)
   * @param data A JSON array of row data (each row represented as JSON arrays).
   */
  void addRows(const dx::JSON &data); // For automatic part ID generation

  /**
   * Queries an open remote table and finds a valid unused number
   * (part ID) which can then be used to add rows to the remote table.
   * The method will not return the same part ID more than once.
   *
   * @return An integer that has not yet been used to upload
   * rows to the remote table object
   */
  int getUnusedPartID();

  /**
   * Ensures that all pending addRows request (including the ones in internal buffer)
   * are completed by worker threads. Blocks until then. Terminates all worker threads.
   * 
   * @note Since this function terminates the thread pool at the end. Thus it is wise 
   * to use it less frequently (for ex: at the end of all addRows(const dx::JSON&) requests, 
   * to ensure that data is actually uploaded to remote file).
   */
  void flush();

  /**
   * Calls flush() and issue request for closing the remote table.
   *
   * @param block If true, waits until the table has finished closing
   * before returning.  Otherwise, it returns immediately.
   */
  void close(const bool block=false) ;

  /**
   * Waits until the remote table is in the "closed" state.
   */
  void waitOnClose() const;
  
  ~DXGTable() {
    flush();
    stopLinearQuery();
  }
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
  DXGTable clone(const std::string &dest_proj_id,
                 const std::string &dest_folder="/") const;

  static DXGTable openDXGTable(const std::string &dxid,
			       const std::string &project="default");

  static DXGTable newDXGTable(const std::vector<dx::JSON> &columns,
                              const std::vector<dx::JSON> &indices,
                              const dx::JSON &data_obj_fields=
                              dx::JSON(dx::JSON_OBJECT));

  static DXGTable newDXGTable(const std::vector<dx::JSON> &columns,
                              const dx::JSON &data_obj_fields=
                              dx::JSON(dx::JSON_OBJECT)) {
    return newDXGTable(columns, std::vector<dx::JSON>(), data_obj_fields);
  }

  static DXGTable newDXGTable(const DXGTable &init_from,
			      const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  static DXGTable newDXGTable(const DXGTable &init_from,
			      const std::vector<dx::JSON> &columns,
			      const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  static DXGTable newDXGTable(const DXGTable &init_from,
			      const std::vector<dx::JSON> &columns,
			      const std::vector<dx::JSON> &indices,
			      const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  static DXGTable extendDXGTable(const DXGTable &dxgtable,
                                 const std::vector<dx::JSON> &columns,
                                 const std::vector<dx::JSON> &indices,
                                 const dx::JSON &data_obj_fields=
                                 dx::JSON(dx::JSON_OBJECT));

  static DXGTable extendDXGTable(const DXGTable &dxgtable,
                                 const std::vector<dx::JSON> &columns,
                                 const dx::JSON &data_obj_fields=
                                 dx::JSON(dx::JSON_OBJECT)) {
    return extendDXGTable(dxgtable, columns, std::vector<dx::JSON>(),
                          data_obj_fields);
  }

  /**
   * Constructs a column descriptor from a column name and data type.
   *
   * @param name Name of the column
   * @param type Data type to be stored in the column
   * @return A JSON object containing the column descriptor
   */
  static dx::JSON columnDesc(const std::string &name,
                             const std::string &type);

  /**
   * Creates a genomic range index descriptor for use with the new()
   * call.
   *
   * @param chr Name of the column containing chromosome names; must
   * be a column of type string
   * @param lo Name of the column containing the low boundary of a
   * genomic interval; must be a column of type int32
   * @param hi Name of the column containing the high boundary of a
   * genomic interval; must be a column of type int32
   * @param name Name of the index
   * @return A JSON object containing the index descriptor
   */
  static dx::JSON genomicRangeIndex(const std::string &chr,
                                    const std::string &lo,
                                    const std::string &hi,
                                    const std::string &name="gri");
  /**
   * Creates a lexicographic index descriptor for use with the new()
   * call.
   *
   * @param columns Vector of lists of the form [<column name>, "ASC"|"DESC"]
   * @param name Name of the index
   * @return A JSON object containing the index descriptor
   */
  static dx::JSON lexicographicIndex(const std::vector<std::vector<std::string> > &columns,
                                     const std::string &name);

  /**
   * Creates a substring index descriptor for use with the new() call.
   *
   * @param column Column name to index by
   * @param name Name of the index
   * @return A JSON object containing the index descriptor
   */
  static dx::JSON substringIndex(const std::string &column,
                                 const std::string &name);

  // TODO: lo and hi are currently constrained by the API spec to be
  // columns of type int32, but if we ever change this, we should
  // change the data type as well.
  /**
   * Constructs a query for a genomic range index of the table.
   *
   * @param chr Name of chromosome to be queried
   * @param lo Low boundary of query interval
   * @param hi High boundary of query interval
   * @param mode The type of query to perform ("overlap" or "enclose")
   * @param index Name of the genomic range index to use
   * @return A JSON object containing the query for use with getRows()
   */
  static dx::JSON genomicRangeQuery(const std::string &chr,
                                    const int64_t lo,
                                    const int64_t hi,
                                    const std::string &mode="overlap",
                                    const std::string &index="gri");

  /**
   * Constructs a query for a lexicographic index of the table.
   *
   * @param query MongoDB-style query
   * @param index Name of the lexicographic index to use
   * @return A JSON object containing the query for use with getRows()
   */
  static dx::JSON lexicographicQuery(const dx::JSON &query,
                                     const std::string &index);

  /**
   * Constructs a query for a substring index of the table.
   *
   * @param match String to match
   * @param mode Mode in which to match the string ("equal", "substring", or "prefix")
   * @param index Name of the substring index to use
   * @return A JSON object containing the query for use with getRows()
   */
  static dx::JSON substringQuery(const std::string &match,
                                 const std::string &mode,
                                 const std::string &index);
};

#endif
