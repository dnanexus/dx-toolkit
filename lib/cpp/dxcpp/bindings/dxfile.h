#ifndef DXCPP_BINDINGS_DXFILE_H
#define DXCPP_BINDINGS_DXFILE_H

#include <fstream>
#include <sstream>
#include "../bindings.h"

/**
 * TODO: Talk about how "open" is only for writing, "closed" is only
 * for reading, and "closing" is a useless state.
 */
class DXFile: public DXDataObject {
 private:
  dx::JSON describe_(const std::string &input_params) const {
    return fileDescribe(dxid_, input_params);
  }
  void addTypes_(const std::string &input_params) const {
    fileAddTypes(dxid_, input_params);
  }
  void removeTypes_(const std::string &input_params) const {
    fileRemoveTypes(dxid_, input_params);
  }
  dx::JSON getDetails_(const std::string &input_params) const {
    return fileGetDetails(dxid_, input_params);
  }
  void setDetails_(const std::string &input_params) const {
    fileSetDetails(dxid_, input_params);
  }
  void setVisibility_(const std::string &input_params) const {
    fileSetVisibility(dxid_, input_params);
  }
  void rename_(const std::string &input_params) const {
    fileRename(dxid_, input_params);
  }
  void setProperties_(const std::string &input_params) const {
    fileSetProperties(dxid_, input_params);
  }
  void addTags_(const std::string &input_params) const {
    fileAddTags(dxid_, input_params);
  }
  void removeTags_(const std::string &input_params) const {
    fileRemoveTags(dxid_, input_params);
  }
  void close_(const std::string &input_params) const {
    fileClose(dxid_, input_params);
  }
  dx::JSON listProjects_(const std::string &input_params) const {
    return fileListProjects(dxid_, input_params);
  }

  /**
   * For use when reading closed remote files; stores the current
   * position (in bytes from the beginning of the file) from which
   * future read() calls will begin.
   */
  int pos_;

  /**
   * Stores the number of bytes read in the last call to read().
   */
  int gcount_;

  /**
   * For use when reading closed remote files; stores length of the
   * file so that accurate byte ranges can be requested.
   */
  int file_length_;

  /**
   * For use when writing remote files; stores a buffer of data that
   * will be periodically flushed to the API server.
   */
  std::string buffer_;

  /**
   * For use when writing remote files; stores the part index to be
   * used on the next part to be uploaded to the API server.
   */
  int cur_part_;

  /**
   * Indicates when end of file has been reached when reading a remote
   * file.
   */
  bool eof_;

  void init_internals_();

  // TODO: Determine if this should be user-defined.
  static const int max_buf_size_;

 public:
  DXFile() {}
  DXFile(const std::string &dxid) { setID(dxid); }

  // File-specific functions

  /**
   * Sets the remote object ID associated with the remote file
   * handler.  If the handler had data stored in its internal buffer
   * to be written to the remote file, that data will be flushed.
   *
   * @param dxid Object ID of the remote file to be accessed
   */
  void setID(const std::string &dxid);

  /**
   * Creates a new remote file object.  Sets the object ID for the
   * DXFile instance which can then be used for writing only.
   *
   * @param media_type String representing the media type of the file.
   */
  void create(const std::string &media_type="");

  /**
   * Reads the next n bytes in the remote file object (or however many
   * are left in the file if there are fewer than n), and stores the
   * downloaded data at ptr.  Note that eof() will return whether the
   * end of the file was reached, and gcount() will return how many
   * bytes were actually read.
   *
   * @param ptr Location to which data should be written
   * @param n The maximum number of bytes to retrieve
   */
  void read(char* ptr, int n);

  /**
   * @return The number of bytes read by the last call to read().
   */
  int gcount() const;

  /**
   * When reading a remote file, returns whether the end of the file
   * has been reached.  If the end of the file has been reached but
   * seek() has been called to set the cursor to appear before the end
   * of the file, then the flag is unset.
   *
   * @return Boolean: true if the end of the file has been reached;
   * false otherwise.
   */
  bool eof() const;

  /**
   * Changes the position of the reading cursor to the specified byte
   * location.  Note that writing is append-only, so calling this
   * function when writing will fail.
   *
   * @param pos New byte position of the read cursor
   */
  void seek(const int pos);

  /**
   * Appends the contents of the internal buffer to the remote file.
   */
  void flush();

  /**
   * Appends the data stored at ptr to an internal buffer that is
   * periodically flushed to be appended to the remote file.
   *
   * @param ptr Location of data to be written
   * @param n Number of bytes to write
   */
  void write(const char* ptr, int n);

  /**
   * Appends data to the file.
   *
   * @see DXFile::write(const char*, int)
   *
   * @param data String to write to the file
   */
  void write(const std::string &data);

  /**
   * Uploads data as a part.
   *
   * @see DXFile::uploadPart(const char*, int, int)
   *
   * @param data String containing the data to append
   * @param index Number with which to label the uploaded part
   */
  void uploadPart(const std::string &data, const int index=-1);

  /**
   * Uploads the n bytes stored at ptr to the remote file and appends
   * it to the existing content.  If index is not given, it will not
   * be passed to the API server.
   *
   * @param ptr Pointer to the location of data to be sent
   * @param n The number of bytes to send
   * @param index Number with which to label the part of the file to
   * be uploaded.  This will be automatically generated if the given
   * value is negative.
   */
  void uploadPart(const char* ptr, int n, const int index=-1);

  /**
   * @return Boolean: true if the remote file is in the "open" state.
   */
  bool is_open() const;

  /**
   * @return Boolean: true if the remote file is in the "closed"
   * state.
   */
  bool is_closed() const;

  /**
   * Flushes the buffer and closes the remote file to further writes.
   *
   * @param block Boolean indicating whether the process should block
   * until the remote file is in the "closed" state (true), or not
   * (false).
   */
  void close(const bool block=false);

  /**
   * Waits until the remote file object is in the "closed" state.
   */
  void waitOnClose() const;

  // TODO: Provide streaming operators for all reasonable types
  /**
   * Streaming operator for writing
   */
  template<typename T>
    DXFile & operator<<(const T& x) {
    std::stringstream str_buf;
    str_buf << x;
    write(str_buf.str());
    return *this;
  }

  typedef std::basic_ostream<char, std::char_traits<char> > couttype;
  typedef couttype& (*stdendline)(couttype&);
  DXFile & operator<<(stdendline manipulator) {
    std::stringstream str_buf;
    str_buf << manipulator;
    write(str_buf.str());
    return *this;
  }

  /**
   * Things that need figuring out: 1) Would need to buffer reading.
   * pos_ would have to be managed carefully between uses of >> and
   * read().  2) How many bytes are necessary to grab the next thing?
   * In any case, we'd want a read buffer.  And maybe to store the
   * entire thing as a stringstream??
   */
  template<typename T>
    DXFile & operator>>(const T& x) {
    throw DXNotImplementedError();
  }

  /**
   * Shorthand for creating a DXFile remote file handler with the
   * given object id.
   *
   * @param dxid Object id of the file to open.
   * @return DXFile remote handler for the requested file object
   */
  static DXFile openDXFile(const std::string &dxid);

  /**
   * Shorthand for creating a DXFile remote file handler for a new
   * empty remote file ready for writing.
   *
   * @param media_type String representing the media type of the file.
   * @return DXFile remote file handler for a new remote file
   */
  static DXFile newDXFile(const std::string &media_type="");

  /**
   * Shorthand for downloading a remote file to a local location.
   *
   * @param dxid Object id of the file to download.
   * @param filename Local path for writing the downloaded data.
   * @param chunksize Size of the chunks with which to divide up the
   * download (in bytes).
   */
  static void downloadDXFile(const std::string &dxid, const std::string &filename, int chunksize=1048576);

  /**
   * Shorthand for uploading a local file and closing it when done.
   *
   * TODO: Decide whether to provide blocking.
   *
   * @param filename Local path for the file to upload.
   * @param media_type String representing the media type of the file.
   * @return DXFile remote file handler for the newly uploaded file.
   */
  static DXFile uploadLocalFile(const std::string &filename, const std::string &media_type="");

  /**
   * TODO: Consider writing a uploadString or uploadCharBuffer which
   * uploads the data and closes it after.
   */
};

#endif
