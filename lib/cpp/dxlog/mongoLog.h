#ifndef DXLOGMONGO_H
#define DXLOGMONGO_H

#include <iostream>
#include "mongo/client/dbclient.h"

using namespace mongo;

namespace DXLog {
  // Wrapper for writing logs to mongo DB
  class MongoWriter {
    private:
      // Server and name of database
      static string server, db;

      // Connection to the database
      static DBClientConnection conn(true);

      // Initialized the above static variables based on execution environment
      static void initEnv();

      // A boolean value to make sure that only one db connection within a process
      static bool connected;
      
    public:
      static bool write(const BSONObj &msg, const string &collection, string &errMsg);
}

#endif
