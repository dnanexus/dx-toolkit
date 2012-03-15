#ifndef DXCPP_BINDINGS_DXFILE_H
#define DXCPP_BINDINGS_DXFILE_H

#import "../bindings.h"

namespace dxpy {
  using namespace dxpy;

  class DXFile: public DXClass {
  public:
    JSON describe() { return fileDescribe(dxid); }
    JSON getProperties() { return fileGetProperties(dxid); }
    void setProperties() { fileSetProperties(dxid); }
    void addTypes() { fileAddTypes(dxid); }
    void removeTypes() { fileRemoveTypes(dxid); }
    void destroy() { fileDestroy(dxid); }

    // File-specific functions

    void setID(string dxid_);
    void create();
    string read();
    void seek();
    void flush();
    void write();
    void upload_part();
    bool is_open();
    void close();
    void wait_on_close();

    // TODO: Provide streaming operators for all reasonable types
    DXFile & operator<<(bool foo);
    DXFile & operator>>(bool foo);
  };

  DXFile openDXFile(string dxid);

  DXFile newDXFile(string mediaType=string());

  void downloadDXFile(string dxid, string filename, int chunksize=1048576);

  DXFile uploadLocalFile(string filename, string media_type=string());

  DXFile uploadString(string to_upload, string media_type=string());

}

#endif
