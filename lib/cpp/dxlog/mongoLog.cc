#include <iostream>
#include "mongo/client/dbclient.h"

bool DXLog::MongoWriter::connected = false;

void DxLog::MongoWriter::initEnv() {
  server = "localhost"; db = "log";
}

bool DXLog::MongoWriter::write(const BSONObj &msg, const string &collection, string &errMsg) {
  try{
    if (! connected) {
      initEnv();
      conn.connect(server);
      connected = true;
    }

    conn.insert(db + "." + collection);
    return true;
  } catch ( DBException &e ) {
    errMsg = e.what();
    return false;
  }
}
