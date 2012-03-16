#ifndef DXCPP_BINDINGS_DXFILE_H
#define DXCPP_BINDINGS_DXFILE_H

#include "../bindings.h"

class DXFile: public DXClass {
 private:
  int pos_; /** For use when reading closed remote files; stores the
	     * current position (in bytes from the beginning of the
	     * file) from which future read() calls will begin.
	     */
  int file_length_; /** For use when reading closed remote files;
			 *  stores length of the file so that accurate
			 *  byte ranges can be requested.
			 */
  string buffer_; /** For use when writing remote files; stores a
		   * buffer of data that will be periodically flushed
		   * to the API server.
		   */
  int cur_part_; /** For use when writing remote files; stores the
		      * part index to be used on the next part to be
		      * uploaded to the API server.
		      */

 public:
  /** Describes the object.
   * @see DXClass::describe()
   */
  JSON describe() const { return fileDescribe(dxid_); }
  JSON getProperties(const JSON &keys) const { return fileGetProperties(dxid_, keys); }
  void setProperties(const JSON &properties) const { fileSetProperties(dxid_, properties); }
  void addTypes(const JSON &types) const { fileAddTypes(dxid_, types); }
  void removeTypes(const JSON &types) const { fileRemoveTypes(dxid_, types); }
  void destroy() { fileDestroy(dxid_); }

  // File-specific functions

  void setID(const string &dxid);
  void create();
  string read();
  void seek();
  void flush();
  void write();
  void upload_part();
  bool is_open() const;
  void close(const bool block=false) const;
  void wait_on_close() const;

  // TODO: Provide streaming operators for all reasonable types
  DXFile & operator<<(bool foo);
  DXFile & operator>>(bool foo);
};

DXFile openDXFile(const string &dxid);

DXFile newDXFile(const string &mediaType="");

void downloadDXFile(const string &dxid, const string &filename, int chunksize=1048576);

DXFile uploadLocalFile(const string &filename, const string &media_type="");

DXFile uploadString(const string &to_upload, const string &media_type="");

#endif
