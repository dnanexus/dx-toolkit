#include "dxgtable.h"
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>

using namespace std;
using namespace dx;


void DXGTable::reset_buffer_() {
  row_buffer_.str("");
  row_buffer_ << "{\"data\": [";
}

void DXGTable::reset() {
  flush();
  reset_buffer_();
  stopLinearQuery();
  countThreadsNotWaitingOnConsume = 0;
  countThreadsWaitingOnConsume = 0;
}

void DXGTable::setIDs(const string &dxid,
		      const string &proj) {
  reset();
  DXDataObject::setIDs(dxid, proj);
}

void DXGTable::setIDs(const char *dxid, const char *proj) {
  if (proj == NULL) {
    setIDs(string(dxid));
  } else {
    setIDs(string(dxid), string(proj));
  }
}

void DXGTable::setIDs(const JSON &dxlink) {
  reset();
  DXDataObject::setIDs(dxlink);
}

void DXGTable::create(const vector<JSON> &columns,
                      const vector<JSON> &indices,
		      const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  if (!data_obj_fields.has("project"))
    input_params["project"] = g_WORKSPACE_ID;
  input_params["columns"] = columns;
  if (indices.size() > 0) {
    input_params["indices"] = indices;
  }

  const JSON resp = gtableNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

void DXGTable::create(const DXGTable &init_from,
		      const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  input_params["initializeFrom"] = JSON(JSON_OBJECT);
  input_params["initializeFrom"]["id"] = init_from.getID();
  input_params["initializeFrom"]["project"] = init_from.getProjectID();
  if (!data_obj_fields.has("project")) {
    input_params["project"] = g_WORKSPACE_ID;
  }

  const JSON resp = gtableNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

void DXGTable::create(const DXGTable &init_from,
		      const vector<JSON> &columns,
		      const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  input_params["initializeFrom"] = JSON(JSON_OBJECT);
  input_params["initializeFrom"]["id"] = init_from.getID();
  input_params["initializeFrom"]["project"] = init_from.getProjectID();
  if (!data_obj_fields.has("project")) {
    input_params["project"] = g_WORKSPACE_ID;
  }
  input_params["columns"] = columns;

  const JSON resp = gtableNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

void DXGTable::create(const DXGTable &init_from,
		      const vector<JSON> &columns,
		      const vector<JSON> &indices,
		      const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  input_params["initializeFrom"] = JSON(JSON_OBJECT);
  input_params["initializeFrom"]["id"] = init_from.getID();
  input_params["initializeFrom"]["project"] = init_from.getProjectID();
  if (!data_obj_fields.has("project")) {
    input_params["project"] = g_WORKSPACE_ID;
  }
  if (columns.size() > 0) {
    input_params["columns"] = columns;
  }
  input_params["indices"] = indices;

  const JSON resp = gtableNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

DXGTable DXGTable::extend(const vector<JSON> &columns,
                          const vector<JSON> &indices,
                          const JSON &data_obj_fields) const {
  JSON input_params = data_obj_fields;
  if (!data_obj_fields.has("project"))
    input_params["project"] = g_WORKSPACE_ID;
  input_params["columns"] = columns;
  if (indices.size() > 0) {
    input_params["indices"] = indices;
  }

  const JSON resp = gtableExtend(dxid_, input_params);

  return DXGTable(resp["id"].get<string>(), input_params["project"].get<string>());
}

JSON DXGTable::getRows(const JSON &query, const JSON &column_names,
                       const int64_t starting, const int64_t limit) const {
  JSON input_params(JSON_OBJECT);
  if (query.type() != JSON_NULL)
    input_params["query"] = query;
  if (column_names.type() == JSON_ARRAY)
    input_params["columns"] = column_names;
  if (starting >= 0)
    input_params["starting"] = starting;
  if (limit >= 0)
    input_params["limit"] = limit;

  return gtableGet(dxid_, input_params);
}

/////////////////////////////////////////////////////////////

void DXGTable::startLinearQuery(const dx::JSON &column_names,
                      const int64_t start_row,
                      const int64_t num_rows,
                      const int64_t chunk_size,
                      const unsigned max_chunks,
                      const unsigned thread_count) const {
  stopLinearQuery(); // Stop any previously running linear query
  
  lq_columns_ = column_names;
  lq_query_start_ = (start_row == -1) ? 0 : start_row;
  lq_query_end_ = (num_rows == -1) ? describe()["length"].get<int64_t>() : lq_query_start_ + num_rows;
  lq_chunk_limit_ = chunk_size;
  lq_max_chunks_ = max_chunks;
  lq_next_result_ = lq_query_start_;
  lq_results_.clear();

  for (unsigned i = 0; i < thread_count; ++i)
    lq_readThreads_.push_back(boost::thread(boost::bind(&DXGTable::readChunk_, this)));
}

void DXGTable::readChunk_() const {
  int64_t start;
  while (true) {
    boost::mutex::scoped_lock qs_lock(lq_query_start_mutex_);
    if (lq_query_start_ >= lq_query_end_)
      break; // We are done fetching all chunks

    start = lq_query_start_;
    lq_query_start_ += lq_chunk_limit_;
    qs_lock.unlock();

    int64_t limit_for_req = std::min(lq_chunk_limit_, (lq_query_end_ - start));

    // Perform the actual query
    JSON ret = getRows(JSON(JSON_NULL), lq_columns_, start, limit_for_req);
    boost::mutex::scoped_lock r_lock(lq_results_mutex_);
    while (lq_next_result_ != start && lq_results_.size() >= lq_max_chunks_) {
      r_lock.unlock();
      boost::this_thread::sleep(boost::posix_time::milliseconds(1));
      r_lock.lock();
    }
    lq_results_[start] = ret["data"];
    r_lock.unlock();
    boost::this_thread::interruption_point();
  }
}

bool DXGTable::getNextChunk(JSON &chunk) const {
  if (lq_readThreads_.size() == 0) // Linear query was not called
    return false;

  boost::mutex::scoped_lock r_lock(lq_results_mutex_);
  if (lq_next_result_ >= lq_query_end_)
    return false;
  while (lq_results_.size() == 0 || (lq_results_.begin()->first != lq_next_result_)) {
    r_lock.unlock();
    usleep(100);
    r_lock.lock();
  }
  chunk = lq_results_.begin()->second;
  lq_results_.erase(lq_results_.begin());
  lq_next_result_ += chunk.size();
  r_lock.unlock();
  return true;
}

void DXGTable::stopLinearQuery() const {
  if (lq_readThreads_.size() == 0)
    return;
  for (unsigned i = 0; i < lq_readThreads_.size(); ++i) {
    lq_readThreads_[i].interrupt();
    lq_readThreads_[i].join();
  }
  lq_readThreads_.clear();
  lq_results_.clear();
}

///////////////////////////////////////////////////////////////////////////

void DXGTable::addRows(const JSON &data, int part_id) {
  JSON input_params(JSON_OBJECT);
  input_params["data"] = data;
  input_params["part"] = part_id;
  gtableAddRows(dxid_, input_params);
}

void DXGTable::finalizeRequestBuffer_() {
  /* This function "closes" the stringified JSON array we are keeping for
   * for buffering requests. It also creates the worker thread if not done previously
   */
  int64_t pos = row_buffer_.tellp();
  if (pos > 10) {
    row_buffer_.seekp(pos - 1); // Erase the trailing comma
    row_buffer_ << "], \"part\": " << getUnusedPartID() << "}"; 
  }
  // We need to create thread pool only once (i.e., if it doesn't exist already)
  if (writeThreads.size() == 0)
    createWriteThreads_();
}

void DXGTable::joinAllWriteThreads_() {
  /* This function ensures that all pending requests are executed and all
   * worker thread are closed after that
   * Brief notes about functioning:
   * --> addRowRequestsQueue.size() == 0, ensures that request queue is empty, i.e.,
   *     some worker has picked the last request (note we use term "pick", because 
   *     the request might still be executing the request).
   * --> Once we know that request queue is empty, we issue interrupt() to all threads
   *     Note: interrupt() will only terminate threads, which are waiting on new request.
   *           So only threads which are blocked by .consume() operation will be terminated
   *           immediatly.
   * --> Now we use a condition based on two interleaved counters to wait until all the 
   *     threads have finished the execution. (see writeChunk_() for understanding their usage)
   * --> Once we are sure that all threads have finished the requests, we join() them.
   *     Since interrupt() was already issued, thus join() terminates them instantly.
   *     Note: Most of them would have been already terminated (since after issuing 
   *           interrupt(), they will be terminated when they start waiting on consume()). 
   *           It's ok to join() terminated threads.
   * --> We clear the thread pool (vector), and reset the counters.
   */

  if (writeThreads.size() == 0)
    return; // Nothing to do (no thread has been started)
  
  // To avoid race condition
  // particularly the case when produce() has been called, but thread is still waiting on consume()
  // we don't want to incorrectly issue interrupt() that time
  while (addRowRequestsQueue.size() != 0) {
    usleep(100);
  }

  for (unsigned i = 0; i < writeThreads.size(); ++i)
    writeThreads[i].interrupt();

  boost::mutex::scoped_lock cl(countThreadsMutex); 
  cl.unlock();
  while (true) {
    cl.lock();
    if ((countThreadsNotWaitingOnConsume == 0) &&
        (countThreadsWaitingOnConsume == (int) writeThreads.size())) {
      cl.unlock();
      break;
    }
    cl.unlock();
    usleep(100);
  }

  for (unsigned i = 0; i < writeThreads.size(); ++i)
    writeThreads[i].join();
  
  writeThreads.clear();
  // Reset the counts
  countThreadsWaitingOnConsume = 0;
  countThreadsNotWaitingOnConsume = 0;
}

// This function is what each of the worker thread executes
void DXGTable::writeChunk_(string gtableId) {
  try {
    boost::mutex::scoped_lock cl(countThreadsMutex); 
    cl.unlock();
    /* This function is executed throughtout the lifetime of an addRows worker thread
     * Brief note about various constructs used in the function:
     * --> addRowRequestsQueue.consume() will block if no pending requests to be
     *     excuted are available.
     * --> gtableAddRows() does the actual upload of rows.
     * --> We use two interleaved counters (countThread{NOT}WaitingOnConsume) to
     *     know when it is safe to terminate the threads (see joinAllWriteThreads_()).
     *     We want to terminate only when thread is waiting on .consume(), and not
     *     when gtableAddRows() is being executed.
     */
     // See C++11 working draft for details about atomics (used for counterS)
     // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2012/n3337.pdf
    while (true) {
      cl.lock();
      countThreadsWaitingOnConsume++;
      cl.unlock();
      std::string req = addRowRequestsQueue.consume();
      cl.lock();
      countThreadsNotWaitingOnConsume++;
      countThreadsWaitingOnConsume--;
      cl.unlock();
      gtableAddRows(gtableId, req);
      cl.lock();
      countThreadsNotWaitingOnConsume--;
      cl.unlock();
    }
  } 
  catch (const boost::thread_interrupted &ti) 
  {
    return;
  }
}

/* This function creates new worker thread for addRows
 * Usually it wil be called once for a series of addRows() and then close() request
 * However, if we call flush() in between, then we destroy any existing threads, thus 
 * threads will be recreated for any further addRows() request
 */
void DXGTable::createWriteThreads_() {
  if (writeThreads.size() == 0) {
    for (int i = 0; i < MAX_WRITE_THREADS; ++i) {
      writeThreads.push_back(boost::thread(boost::bind(&DXGTable::writeChunk_, this, _1), dxid_));
    }
  }
}

void DXGTable::addRows(const JSON &data) {
  for (JSON::const_array_iterator iter = data.array_begin();
       iter != data.array_end();
       iter++) {
    row_buffer_ << (*iter).toString() << ",";

    if (row_buffer_.tellp() >= row_buffer_maxsize_) {
      finalizeRequestBuffer_();
      addRowRequestsQueue.produce(row_buffer_.str());
      reset_buffer_();  
    }
  }
}

int DXGTable::getUnusedPartID() {
  const JSON resp = gtableNextPart(dxid_);
  return resp["part"].get<int>();
}

void DXGTable::flush() {
  int64_t pos = row_buffer_.tellp();
  if (pos > 10) {
    finalizeRequestBuffer_();
    addRowRequestsQueue.produce(row_buffer_.str());
  }
  joinAllWriteThreads_();
  reset_buffer_();
}

void DXGTable::close(const bool block) {
  flush();
  gtableClose(dxid_);

  if (block)
    waitOnState();
}

void DXGTable::waitOnClose() const {
  waitOnState();
}

DXGTable DXGTable::openDXGTable(const string &dxid,
				const string &proj) {
  return DXGTable(dxid, proj);
}

DXGTable DXGTable::newDXGTable(const vector<JSON> &columns,
                               const vector<JSON> &indices,
                               const JSON &data_obj_fields) {
  DXGTable gtable;
  gtable.create(columns, indices, data_obj_fields);
  return gtable;
}

DXGTable DXGTable::newDXGTable(const DXGTable &init_from,
			       const JSON &data_obj_fields) {
  DXGTable gtable;
  gtable.create(init_from, data_obj_fields);
  return gtable;
}

DXGTable DXGTable::newDXGTable(const DXGTable &init_from,
                               const vector<JSON> &columns,
                               const JSON &data_obj_fields) {
  DXGTable gtable;
  gtable.create(init_from, columns, data_obj_fields);
  return gtable;
}

DXGTable DXGTable::newDXGTable(const DXGTable &init_from,
                               const vector<JSON> &columns,
                               const vector<JSON> &indices,
                               const JSON &data_obj_fields) {
  DXGTable gtable;
  gtable.create(init_from, columns, indices, data_obj_fields);
  return gtable;
}

DXGTable DXGTable::extendDXGTable(const DXGTable &dxgtable,
                                  const vector<JSON> &columns,
                                  const vector<JSON> &indices,
                                  const JSON &data_obj_fields) {
  return dxgtable.extend(columns, indices, data_obj_fields);
}

JSON DXGTable::columnDesc(const string &name,
                          const string &type) {
  JSON col_desc(JSON_OBJECT);
  col_desc["name"] = name;
  col_desc["type"] = type;
  return col_desc;
}

JSON DXGTable::genomicRangeIndex(const string &chr,
                                 const string &lo,
                                 const string &hi,
                                 const string &name) {
  JSON index_desc(JSON_OBJECT);
  index_desc["name"] = name;
  index_desc["type"] = "genomic";
  index_desc["chr"] = chr;
  index_desc["lo"] = lo;
  index_desc["hi"] = hi;
  return index_desc;
}

JSON DXGTable::lexicographicIndex(const vector<vector<string> > &columns,
                                  const string &name) {
  JSON index_desc(JSON_OBJECT);
  index_desc["name"] = name;
  index_desc["type"] = "lexicographic";
  index_desc["columns"] = columns;
  return index_desc;
}

JSON DXGTable::genomicRangeQuery(const string &chr,
                                 const int64_t lo,
                                 const int64_t hi,
                                 const string &mode,
                                 const string &index) {
  JSON query(JSON_OBJECT);
  query["index"] = index;
  query["parameters"] = JSON(JSON_OBJECT);
  query["parameters"]["mode"] = mode;
  query["parameters"]["coords"] = JSON(JSON_ARRAY);
  query["parameters"]["coords"].push_back(chr);
  query["parameters"]["coords"].push_back(lo);
  query["parameters"]["coords"].push_back(hi);
  return query;
}

JSON DXGTable::lexicographicQuery(const JSON &mongo_query,
                                  const string &index) {
  JSON query(JSON_OBJECT);
  query["index"] = index;
  query["parameters"] = mongo_query;
  return query;
}

DXGTable DXGTable::clone(const string &dest_proj_id,
                         const string &dest_folder) const {
  clone_(dest_proj_id, dest_folder);
  return DXGTable(dxid_, dest_proj_id);
}
