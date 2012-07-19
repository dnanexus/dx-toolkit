#ifndef DXLOG_H
#define DXLOG_H

#define defaultPrioritySocket "/opt/dnanexus/log/priority"
#define defaultBulkSocket "/opt/dnanexus/log/bulk"

#include "dxLog_helper.h"
#include "unixDGRAM.h"

using namespace std;

namespace DXLog {
  enum Level { EMERG, ALERT, CRIT, ERR, WARN, NOTICE, INFO, DEBUG };

  // Read a json object from a file
  dx::JSON readJSON(const string &filename);
  
  /** Validate that message has proper data for logging based on information specified in schema
   *  Return false if failed and errMsg has detailed information of error
   */
//  bool ValidateLogData(dx::JSON &message, string &errMsg); 

  // API for writing logs to rsyslog and/or mongodb
  class logger {
    private:
      string hostname;

    public:
      logger() { hostname = getHostname(); }

      /** Send log message to rsyslog and/or mongodb.
      *  data is a hash contains the log message and related information 
      *  Reture whether or not if the message was sent successfully.
      *  Otherwise return false and errMsg contains detailed error message
      */
      bool Log(dx::JSON &data, string &errMsg, const string &socketPath = "/dev/log"); 
  };

  // Wrapper for writing logs from an app
  class AppLog {
    private: 
      static bool initialized;

      // Data associated with messages obtained from execution environment
      static string socketPath[2];
      //static dx::JSON schema;
      
      // Determine which rsyslog socket to use for message with this level
      static int socketIndex(int level);

    public:
      // Set the above static variables based on execution environment
      static bool initEnv(const dx::JSON &conf, string &errMsg);

      /** The public log function for any app
      *  Returns true if the message is successfully delivered to the log system;
      *  Otherwise returns false and errMsg contains some details of the error
      */
      static bool log(const string &message, int level = 6);
      static bool emerg(const string &message) { return log(message, 0); }
      static bool alert(const string &message) { return log(message, 1); }
      static bool crit(const string &message) { return log(message, 2); }
      static bool error(const string &message) { return log(message, 3); }
      static bool warn(const string &message) { return log(message, 4); }
      static bool notice(const string &message) { return log(message, 5); }
      static bool info(const string &message) { return log(message, 6); }
      static bool debug(const string &message) { return log(message, 7); }

      static bool done(string &errMsg);
  };

  // bool Log(int facility, int level, const string &tag, const string &msg, string &errMsg);
};

#endif
