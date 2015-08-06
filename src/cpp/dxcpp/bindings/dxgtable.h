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
 * \brief GenomicTables.
 */

#ifndef DXCPP_BINDINGS_DXGTABLE_H
#define DXCPP_BINDINGS_DXGTABLE_H

#include <sstream>
#include <boost/thread.hpp>
#include "../bindings.h"
#include "../api.h"
#include "../bqueue.h"

namespace dx {
  //! A large-scale, immutable, tabular dataset.

  ///
  /// GenomicTables (GTables) are a medium for storing and querying large amounts of tabular data and
  /// querying it (see the <a
  /// href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables">API specification</a>
  /// for more info). In order to support jobs that process large volumes of data, the GTable API
  /// allows streaming storage and retrieval of data, and parallel access from multiple compute
  /// nodes; and can automatically sort and index GTable data for you. GTables can support queries on
  /// genomic coordinates or other indices and can be used to back interactive applications such as
  /// the DNAnexus genome browser.
  ///
  /// GTables have a schema in which each column must have one of the available types (choose from
  /// booleans, strings, and a variety of numeric types).
  ///
  /// The DNAnexus platform uses GTables as a common format for interchange of genomic datasets,
  /// including reads, mappings, and variants.
  ///
  /// When a GTable object is initialized, it is empty, in the "open" state, and writable. GTable
  /// objects may be written in multiple "parts" to support easy failure recovery and parallel
  /// writing. After you have written all the data you like to the GTable, you may <em>close</em> it.
  /// The GTable goes into the "closing" state. If you requested any indices, the table is sorted
  /// according to the index ordering (if any) and any indices requested are generated at this time.
  /// When the GTable is ready for reading, it goes into the "closed" state.
  ///
  /// You can write a GTable object in the following ways, which you may <b>not</b> mix and match:
  ///
  /// - If you are generating a single stream of output, use addRows(const dx::JSON&). Your data is
  ///   automatically buffered and split into parts, and parallel HTTP requests are used to upload it
  ///   to the Platform. The client automatically obtains part IDs in increasing order and your data
  ///   appears in the final GTable in the same order in which it was written (unless you added an
  ///   index to the GTable).
  /// - If you prefer to have explicit control of parallel writes, you can use
  ///   addRows(const dx::JSON&, int). This allows you to generate the GTable data out of order: you
  ///   can specify a part ID to be associated with each write, and the parts are concatenated in
  ///   increasing order of part ID in order to produce the GTable (before indexing, if any). Each
  ///   %addRows(const dx::JSON&, int) call makes one HTTP request and blocks until that part has
  ///   been written.
  ///
  /// When you are finished writing data to the GTable, call close(). If you wish to wait until the
  /// GTable has been closed before proceeding, you can supply the <code>block=true</code> parameter
  /// to close(); call waitOnClose(); or poll the GTable's status yourself, using describe().
  ///
  /// GTables support reading data based on row range or based on a query.
  ///
  /// To read by row range, do one of the following:
  ///
  /// - Use startLinearQuery() to start fetching data in the background (in chunks of a fixed number
  ///   of rows) and getNextChunk() to read it.
  /// - Use getRows() with a null query.
  ///
  /// To read rows that match a particular query, formulate a query with genomicRangeQuery() or
  /// lexicographicQuery() and use getRows() to execute it.
  ///
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
    
    // configurable params
    int64_t row_buffer_maxsize_;
    int max_write_threads_;

    void reset_buffer_();
    void reset_data_processing_();
    void reset_config_variables_();
    void reset_everything_();
    void copy_config_variables_(const DXGTable &to_copy);

    // To allow interleaving (without compiler optimization possibly changing order)
    // we use std::atomic (a c++11 feature)
    // Ref https://parasol.tamu.edu/bjarnefest/program/boehm-slides.pdf (page 7)
    // Update: Since CLang does not support atomics yet, we are using locking 
    //         mechanism with alongwith volatile
    volatile int countThreadsWaitingOnConsume, countThreadsNotWaitingOnConsume;
    boost::mutex countThreadsMutex;
    std::vector<boost::thread> writeThreads;
    static const int DEFAULT_WRITE_THREADS = 5;
    static const int64_t DEFAULT_ROW_BUFFER_MAXSIZE = 104857600; // 100MB
    BlockingQueue<std::string> addRowRequestsQueue;
    
    // For linear query
    mutable std::map<int64_t, dx::JSON> lq_results_;
    mutable dx::JSON lq_columns_;
    mutable int64_t lq_chunk_limit_;
    mutable int64_t lq_query_start_;
    mutable int64_t lq_query_end_;
    mutable unsigned lq_max_chunks_;
    mutable int64_t lq_next_result_;
    mutable std::vector<boost::thread> lq_readThreads_;
    mutable boost::mutex lq_results_mutex_, lq_query_start_mutex_;

  public:

    DXGTable()
      : DXDataObject() 
    {
      reset_everything_();
    }
    
    /**
     * Creates a %DXGTable handler for the specified File object.
     *
     * @param dxid GTable ID.
     * @param proj ID of the project in which to access the object (if NULL, then default workspace will be used).
     */
    DXGTable(const char *dxid, const char *proj=NULL): DXDataObject() 
    {
      reset_everything_();
      setIDs(std::string(dxid), (proj == NULL) ? config::CURRENT_PROJECT() : std::string(proj));
    }
   
    /**
     * Copy constructor.
     */
    DXGTable(const DXGTable &to_copy)
      : DXDataObject() 
    {
      reset_everything_();
      setIDs(to_copy.dxid_, to_copy.proj_);
      copy_config_variables_(to_copy);
    }

    /**
     * Creates a %DXGTable handler for the specified remote GTable.
     *
     * @param dxid GTable ID.
     * @param proj ID of the project in which to access the object.
     */
    DXGTable(const std::string & dxid, const std::string &proj=config::CURRENT_PROJECT())
      : DXDataObject()
    {
      reset_everything_();
      setIDs(dxid, proj);
    }
    
    /**
     * Creates a %DXGTable handler for the specified remote GTable.
     *
     * @param dxlink A JSON representing a <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Details-and-Links#Linking">DNAnexus link</a>.
     *  You may also use the extended form: {"$dnanexus_link": {"project": proj-id, "id": obj-id}}.
     */
    DXGTable(const dx::JSON &dxlink)
      : DXDataObject()
    {
      reset_everything_();
      setIDs(dxlink);
    }


    /**
     * Assignment operator.
     */
    DXGTable& operator=(const DXGTable& to_copy) {
      if (this == &to_copy)
        return *this;

      this->setIDs(to_copy.dxid_, to_copy.proj_); // setIDs will stop any ongoing data processing, i.e., reset_data_processing_()
      this->copy_config_variables_(to_copy);
      return *this;
    }

    /**
     * Returns the buffer size (in bytes, of the stringified row data) that must be reached before
     * rows are automatically flushed.
     *
     * @returns Buffer size, in bytes.
     */
    int64_t getMaxBufferSize() const {
      return row_buffer_maxsize_;
    }

    /**
     * Sets the buffer size (in bytes of the stringified rows) that must
     * be reached before rows are flushed.
     *
     * @param buf_size New buffer size, in bytes, to use
     */
    void setMaxBufferSize(const int64_t buf_size) {
      row_buffer_maxsize_ = buf_size;
    }
    
    /**
     * Returns maximum number of write threads used by parallelized addRows(const dx::JSON&)
     * operation.
     *
     * @returns Number of threads
     */
    int getNumWriteThreads() const {
      return max_write_threads_;
    }

    /**
     * Sets the maximum number of threads used by parallelized addRows(const dx::JSON&)
     * operation.
     *
     * @param numThreads Number of threads
     */
    void setNumWriteThreads(const int numThreads) {
      max_write_threads_ = numThreads;
    }
   
    // Table-specific functions

    /**
     * Sets the remote object ID associated with the remote GTable handler. If the handler had rows
     * queued up in the internal buffer, they are flushed.
     *
     * @param dxid Remote object ID of the remote GTable to be accessed
     * @param proj Project ID of the remote GTable to be accessed.
     */
    void setIDs(const std::string &dxid, const std::string &proj=config::CURRENT_PROJECT());
    
    /**
     * Sets the remote object ID associated with the remote GTable handler. If the handler had rows
     * queued up in the internal buffer, they are flushed.
     *
     * @param dxid Remote object ID of the remote GTable to be accessed
     * @param proj ID of the project in which to access the GTable (if NULL, then default workspace will be used).
     */
    void setIDs(const char *dxid, const char *proj = NULL);

    /**
     * Sets the remote object ID associated with the remote GTable handler. If the handler had rows
     * queued up in the internal buffer, they are flushed.
     *
     * @param dxlink A JSON representing a <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Details-and-Links#Linking">DNAnexus link</a>.
     *  You may also use the extended form: {"$dnanexus_link": {"project": proj-id, "id": obj-id}}.
     */
    void setIDs(const dx::JSON &dxlink);

    /**
     * Creates a new remote GTable and sets the object ID.
     *
     * @param columns Vector of column descriptors. This must be nonempty.
     * @param indices Vector of index descriptors.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     */
    void create(const std::vector<dx::JSON> &columns,
                const std::vector<dx::JSON> &indices,
                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

    /**
     * Creates a new remote GTable with no indices and sets the object ID.
     *
     * @param columns Vector of column descriptors. This must be nonempty.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     */
    void create(const std::vector<dx::JSON> &columns,
                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT)) {
      create(columns, std::vector<dx::JSON>(), data_obj_fields);
    }

    /**
     * Creates a new remote GTable, initializing it using the schema and metadata from an existing
     * GTable, and sets the object ID.
     *
     * Note that the default behavior of creating a new data object in the current workspace is still
     * in effect and "project" needs to be explicitly specified if the project of the object
     * specified as init_from (or any other project) is to be used.
     *
     * @param init_from A GTable from which to initialize all metadata, including column and index
     * specs.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     */
    void create(const DXGTable &init_from,
                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

    /**
     * Creates a new remote GTable, initializing it using the schema and metadata from an existing
     * GTable and overriding the columns, and sets the object ID.
     *
     * @param init_from A GTable from which to initialize all metadata, including column and index
     * specs.
     * @param columns Vector of column descriptors with which to override the defaults specified by
     * init_from. This must be nonempty.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     */
    void create(const DXGTable &init_from,
                const std::vector<dx::JSON> &columns,
                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

    /**
     * Creates a new remote GTable, initializing it using the schema and metadata from an existing
     * GTable and overriding the columns and indices, and sets the object ID.
     *
     * @param init_from A GTable from which to initialize all metadata, including column and index
     * specs.
     * @param columns Vector of column descriptors with which to override the defaults specified by
     * init_from. Use an empty array to override the index specs while inheriting the columns from
     * init_from.
     * @param indices Vector of index descriptors with which to override the defaults specified in
     * init_from. Unlike columns in this method, an empty array will actually set the index specs to
     * be an empty list.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     */
    void create(const DXGTable &init_from,
                const std::vector<dx::JSON> &columns,
                const std::vector<dx::JSON> &indices,
                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

    /**
     * Retrieves the requested rows and columns.
     *
     * @param query A JSON hash (created with genomicRangeQuery() or lexicographicQuery())
     * representing a query to filter the rows, or JSON_NULL to return all rows in the requested row
     * range.
     * @param column_names A JSON array listing the column names to be returned; the order of the
     * column names will be respected in the output. (Use the value JSON_NULL to indicate all
     * columns.)
     * @param starting An integer representing the first row ID to return (or the row ID to begin
     * searching at, if a query is supplied).
     * @param limit An integer representing the maximum number of rows to be returned.
     *
     * @return A JSON hash with keys "length", "next", and "data", containing, respectively, the
     * number of rows returned, the next row ID that may satisfy the same query (or null if there are
     * known to be no more query results), and the matching results, as an array of arrays. For more
     * info, see the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable-xxxx%2Fget">/gtable-xxxx/get</a>
     * API method.
     */
    dx::JSON getRows(const dx::JSON &query=dx::JSON(dx::JSON_NULL),
                     const dx::JSON &column_names=dx::JSON(dx::JSON_NULL),
                     const int64_t starting=-1, const int64_t limit=-1) const;

    /**
     * Starts fetching rows in chunks of the specified size, in the background. After calling this
     * function, getNextChunk() can be use to access chunks in a linear manner.
     *
     * @note - Calling this function invalidates any previous call to the function.
     *
     * @param column_names A JSON array listing the column names to be returned; the order of the
     * column names will be respected in the output. (Use the JSON null value to indicate all
     * columns.)
     * @param start_row Row number (0-indexed) starting from which rows will be fetched.
     * @param num_rows Maximum number of rows to be fetched
     * @param chunk_size Number of rows to be fetched in each chunk. (Each chunk will have this
     * number of rows, except possibly the last one, which can be shorter.)
     * @param max_chunks Number of fetched chunks to be kept in memory at any time. Note number of
     * real chunks in memory could be as high as (max_chunks + thread_count).
     * @param thread_count Number of threads to be used for fetching rows.
     *
     * @see stopLinearQuery(), getNextChunk()
     */
    void startLinearQuery(const dx::JSON &column_names=dx::JSON(dx::JSON_NULL),
                          const int64_t start_row=0,
                          const int64_t num_rows=-1,
                          const int64_t chunk_size=10000,
                          const unsigned max_chunks=20,
                          const unsigned thread_count=5) const;

    /**
     * Stops background fetching of all chunks. Terminates all read threads. Any previous call to
     * startLinearQuery() is invalidated.
     *
     * This function is idempotent.
     *
     * @see startLinearQuery(), getNextChunk()
     */
    void stopLinearQuery() const;

    /**
     * Obtains the next chunk of rows after a call to startLinearQuery(). Returns false if
     * %startLinearQuery() was not called, or if all the requested chunks from the last call to
     * %startLinearQuery() have been exhausted.
     *
     * @param chunk If the function returns "true", then this object will be populated with rows from
     * next chunk (an array of arrays). Otherwise, this object remains untouched.
     *
     * @return Returns "true" if another chunk is available for processing (in which case the value
     * of chunk is copied into the input argument "chunk"). Returns "false" if all chunks have been
     * exhausted, or no call to startLinearQuery() was made.
     *
     * @see startLinearQuery(), stopLinearQuery()
     */
    bool getNextChunk(dx::JSON &chunk) const;

    /**
     * Adds the rows listed in data to the remote GTable, writing to the specified part ID, as
     * specified in <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable-xxxx%2FaddRows">/gtable-xxxx/addRows</a>.
     *
     * @note This function works quite differently from its overloaded counterpart
     * addRows(const dx::JSON&).
     * - It is not multithreaded and does not use any internal buffer.
     * - It is always blocking, and returns only after the HTTP request finishes.
     *
     * @see getUnusedPartID()
     *
     * @warning In general you should never mix and match calls to addRows(const dx::JSON&, int) and
     * addRows(const dx::JSON&).
     *
     * @param data A JSON array of row data (each row represented by a JSON array).
     * @param part_id The part ID for the uploaded part.
     */
    void addRows(const dx::JSON &data, int part_id);

    /**
     * Appends the rows listed in data to the remote GTable.
     *
     * The data is written to an internal buffer that is added to the GTable when full.
     *
     * For increased throughput, this function uses multiple threads for adding rows in the
     * background. It will block only if the internal buffer is full and all available workers
     * (MAX_WRITE_THREADS threads) are already busy with HTTP requests. Otherwise, it returns
     * immediately.
     *
     * If any of the threads fails then std::terminate() will be called.
     *
     * @warning In general you should never mix and match calls to addRows(const dx::JSON&, int) and
     * addRows(const dx::JSON&).
     *
     * @param data A JSON array of row data (each row represented by a JSON array).
     */
    void addRows(const dx::JSON &data); // For automatic part ID generation

    /**
     * Queries the remote GTable and returns a valid part ID which can then be used to add rows to
     * the GTable. The method will not return the same part ID more than once, even if multiple
     * clients are querying the same GTable simultaneously.
     *
     * However, this method does <b>not</b> check that the part ID that it returns has not previously
     * been written, nor does it protect against someone else adding rows with that part ID.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable-xxxx%2FnextPart">/gtable-xxxx/nextPart</a>
     * API method for more info.
     *
     * @return A valid part ID can be used to upload to the remote GTable object.
     */
    int getUnusedPartID();

    /**
     * Ensures that all the data sent via previous addRows(const dx::JSON&) requests has been flushed
     * from the buffers and added to the remote GTable. Finishes all pending write requests and
     * terminates all write threads. This function blocks until the above has been completed.
     *
     * This function is idempotent.
     *
     * @note Since this function terminates the thread pool, use it sparingly (for example, only you
     * have finished all your addRows(const dx::JSON&) requests, to force the data to be written).
     *
     * @see addRows(const dx::JSON&)
     */
    void flush();

    /**
     * Calls flush() and issues a request to close the remote GTable.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable-xxxx%2Fclose">/gtable-xxxx/close</a>
     * API method for more info.
     *
     * @param block Boolean indicating whether the process should block until the remote GTable is in
     * the "closed" state, or to return immediately (false).
     */
    void close(const bool block=false) ;

    /**
     * Waits until the remote GTable is in the "closed" state.
     */
    void waitOnClose() const;

    ~DXGTable() {
      flush();
      stopLinearQuery();
    }

    /**
     * Clones the associated object into the specified project and folder.
     *
     * @param dest_proj_id ID of the project to which the object should be cloned.
     * @param dest_folder Folder route in which to put it in the destination project.
     *
     * @return New object handler with the associated project set to dest_proj_id.
     */
    DXGTable clone(const std::string &dest_proj_id,
                   const std::string &dest_folder="/") const;

    /**
     * Returns a DXGTable handler for the specified GTable.
     *
     * @param dxid GTable ID.
     * @param project The project in which to open the GTable.
     *
     * @return An object handler for the specified GTable.
     */
    static DXGTable openDXGTable(const std::string &dxid,
                                 const std::string &project="default");

    /**
     * Creates a new remote GTable and returns a handler for it.
     *
     * @param columns Vector of column descriptors; must be nonempty
     * @param indices Vector of index descriptors.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     *
     * @return An object handler for the newly created GTable.
     */
    static DXGTable newDXGTable(const std::vector<dx::JSON> &columns,
                                const std::vector<dx::JSON> &indices,
                                const dx::JSON &data_obj_fields=
                                dx::JSON(dx::JSON_OBJECT));
    /**
     * Creates a new remote GTable and returns a handler for it.
     *
     * Equivalent to: newDXGTable(columns, std::vector<dx::JSON>(), data_obj_fields).
     *
     * @param columns Vector of column descriptors; must be nonempty
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     *
     * @return An object handler for the newly created GTable.
     */
    static DXGTable newDXGTable(const std::vector<dx::JSON> &columns,
                                const dx::JSON &data_obj_fields=
                                dx::JSON(dx::JSON_OBJECT)) {
      return newDXGTable(columns, std::vector<dx::JSON>(), data_obj_fields);
    }

    /**
     * Creates a new remote GTable using the metadata from an existing GTable.
     *
     * Note that the default behavior of creating a new data object in the current workspace is still
     * in effect and "project" needs to be explicitly specified if the project of the object
     * specified as init_from (or any other project) is to be used.
     *
     * @param init_from A GTable from which to initialize all metadata, including column and index
     * specs.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     *
     * @return An object handler for the newly created GTable.
     */
    static DXGTable newDXGTable(const DXGTable &init_from,
                                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

    /**
     * Creates a new remote GTable using the metadata from an existing GTable.
     *
     * Note that the default behavior of creating a new data object in the current workspace is still
     * in effect and "project" needs to be explicitly specified if the project of the object
     * specified as init_from (or any other project) is to be used.
     *
     * @param init_from A GTable from which to initialize all metadata, including column and index
     * specs.
     * @param columns Vector of column descriptors with which to override the defaults specified by
     * init_from.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     *
     * @return An object handler for the newly created GTable.
     */
    static DXGTable newDXGTable(const DXGTable &init_from,
                                const std::vector<dx::JSON> &columns,
                                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));
    /**
     * Creates a new remote GTable using the metadata from an existing GTable.
     *
     * Note that the default behavior of creating a new data object in the current workspace is still
     * in effect and needs to be explicitly stated if the project of the object specified as
     * init_from is to be used.
     *
     * @param init_from A GTable from which to initialize all metadata, including column and index
     * specs.
     * @param columns Vector of column descriptors with which to override the defaults specified by
     * init_from. Use an empty array to override the index specs while inheriting the columns from
     * init_from.
     * @param indices Vector of index descriptors with which to override the defaults specified in
     * init_from. Unlike columns in this method, an empty array will actually set the index specs to
     * be an empty list.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project" if not using the default project, "types", "details", "hidden", "name",
     * "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#API-method%3A-%2Fgtable%2Fnew">/gtable-xxxx/new</a>
     * API method.
     *
     * @return An object handler for the newly created GTable.
     */
    static DXGTable newDXGTable(const DXGTable &init_from,
                                const std::vector<dx::JSON> &columns,
                                const std::vector<dx::JSON> &indices,
                                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

    /**
     * Constructs a column descriptor from a column name and data type.
     *
     * @param name Name of the column.
     * @param type Data type to be stored in the column.
     *
     * @return A JSON object containing the column descriptor
     */
    static dx::JSON columnDesc(const std::string &name,
                               const std::string &type);

    /**
     * Creates a genomic range index descriptor for use with the create() calls.
     *
     * @param chr Name of the column containing chromosome names (must be a column in the GTable, of
     * type string).
     * @param lo Name of the column containing the low boundary of a genomic interval (must be an
     * integral type column in the GTable).
     * @param hi Name of the column containing the high boundary of a genomic interval (must be an
     * integral type column in the GTable).
     * @param name Name of the index.
     *
     * @return A JSON object containing the index descriptor.
     */
    static dx::JSON genomicRangeIndex(const std::string &chr,
                                      const std::string &lo,
                                      const std::string &hi,
                                      const std::string &name="gri");

    /**
     * Creates a lexicographic index descriptor for use with the create() calls.
     *
     * @param columns JSON An array of hashes of the form: {"name": "Col1", "order": "ASC", "caseSensitive": true}. 
     * (Note: fields "order" & "caseSensitive" are optional in the hashes)
     * @param name Name of the index.
     *
     * @return A JSON object containing the index descriptor.
     */
    static dx::JSON lexicographicIndex(const dx::JSON &columns,
                                       const std::string &name);

    // TODO: lo and hi are currently constrained by the API spec to be
    // columns of type int32, but if we ever change this, we should
    // change the data type as well.
    /**
     * Constructs a query for a genomic range index of a GTable.
     *
     * @param chr Name of chromosome to be queried.
     * @param lo Low boundary of query interval.
     * @param hi High boundary of query interval.
     * @param mode The type of query to perform ("overlap" or "enclose").
     * @param index Name of the genomic range index to use.
     *
     * @return A JSON object containing the query for use with getRows().
     */
    static dx::JSON genomicRangeQuery(const std::string &chr,
                                      const int64_t lo,
                                      const int64_t hi,
                                      const std::string &mode="overlap",
                                      const std::string &index="gri");

    /**
     * Constructs a query for a lexicographic index of a GTable.
     *
     * @param query MongoDB-style query.
     * @param index Name of the lexicographic index to use.
     *
     * @return A JSON object containing the query for use with getRows().
     */
    static dx::JSON lexicographicQuery(const dx::JSON &query,
                                       const std::string &index);

    /**
     * Sentinel value that you can use as a null value for numerical
     * columns (equal to -2^31).
     */
    static const int NULL_VALUE;
  };
}
#endif
