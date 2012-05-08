#include "mongoLog.h"

bool DXLog::MongoDriver::connected = false;
string DXLog::MongoDriver::server = "localhost";
string DXLog::MongoDriver::db = "dxlog";
DBClientConnection DXLog::MongoDriver::conn(true);

bool DXLog::MongoDriver::oneOperation(int action, const BSONObj &bson, const string &collection, string &errMsg) {
  try{
    if (! connected) {
      conn.connect(server);
      connected = true;
    }

    string col = db + "." + collection;
    if (action == 1) {
      conn.insert(col, bson);
      string e = conn.getLastError();
      if ( !e.empty()) {
	 errMsg = "Insert failed";
	 return false;
      }
      return true;
    } else if (action == 2) {
      conn.ensureIndex(col, bson);
    }
    return true;
  } catch ( DBException &e ) {
    errMsg = e.what();
    return false;
  }
}
