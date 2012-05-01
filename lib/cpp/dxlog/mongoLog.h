#ifndef DXLOGMONGO_H
#define DXLOGMONGO_H

#include "mongo/client/dbclient.h"

using namespace mongo;

namespace DXLog {
  // Wrapper for writing logs to mongo DB
  class MongoDriver {
    private:
      // Server and name of database
      static string server, db;

      // Connection to the database
      static DBClientConnection conn;

      // A boolean value to make sure that only one db connection within a process
      static bool connected;

      static bool oneOperation(int action, const BSONObj &bson, const string &collection, string &errMsg);
      
    public:
      static void setServer(const string &server_) { server = server_; }
      static void setDB(const string db_) { db = db_; }

      static bool insert(const BSONObj &msg, const string &collection, string &errMsg) { return oneOperation(1, msg, collection, errMsg); }
      static bool ensureIndex(const BSONObj &index, const string &collection, string &errMsg) { return oneOperation(2, index, collection, errMsg); }
  };
};

#endif
