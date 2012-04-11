#ifndef DXLOG_H
#define DXLOG_H

#include <dxjson/dxjson.h>

using namespace std;

namespace DXLog {
  enum Level { EMERG, ALERT, CRIT, ERR, WARN, NOTICE, INFO, DEBUG };

  // Return a string representing the level
  string levelString(int level); 

  // Form message header as <pri> tag. Note that pri is a combination of facility and level and only the first 100 characters of a tag are included
  void formMessageHead(int facility, int level, const string &tag, string &head);

  bool splitMessage(const string &msg, vector<string> &msgs, int msgSize);

  bool SendMessage2Rsyslog(int facility, int level, const string &tag, const string &msg, string &errMsg, int msgSize);
  
  // Wrapper for writing logs from an app
  class AppLog {
    private: 
      /** Each job has two limits of total number of messages
	*  One is for EMERG, ALERT, and CRIT messages
	*  The other is for ERR, WARN, NOTICE, INFO, DEBUG messages
	*  msgCoutn stores current number of messages being stored 
	*/
      static int msgCount[2]; // 

      // Data associated with messages obtained from execution environment
      static string socketPath[2]; // appId maybe
      static dx::JSON data;

      // Set the above static variables based on execution environment
      static bool initEnv();

      // A boolean value indicated whether or not the above static variables are initialized
      static bool initialized;  
      
      // Determine which rsyslog socket to use for message with this level
      static int socketIndex(int level);

    public:
      /** The public log function for any app
	*  Returns true if the message is successfully delivered to the log system;
	*  Otherwise returns false and errMsg contains some details of the error
	*/
      static bool log(const string &msg, string &errMsg, int level = INFO);
  };

//  bool Log(int facility, int level, const string &tag, const string &msg, string &errMsg);
};

#endif
