#ifndef DXLOG_HELPER_H
#define DXLOG_HELPER_H

#include "dxjson.h"

using namespace std;

namespace DXLog {
  void throwString(const string &msg);

  string levelString(int level);
  
  string getHostname();

  string timeISOString(int64 utc);

  // throw a const string with detailed information if there is an error
  void ValidateLogLevel(const dx::JSON &level);
  void ValidateLogFacility(const dx::JSON &facility);
  void ValidateLogRequired(const dx::JSON &required);
  void ValidateLogMsgSize(const dx::JSON &maxMsgSize);
  void ValidateLogText(const dx::JSON &text);
  void ValidateLogMongoDBColumns(const dx::JSON &columns);
  void ValidateLogMongoDBIndex(const dx::JSON &index, const dx::JSON &columns);
  void ValidateLogMongoDBIndexes(const dx::JSON &indexes, const dx::JSON &columns);
  void ValidateLogMongoDB(const dx::JSON &mongodb);
  void ValidateLogSchemaSingle(const dx::JSON &schema);
  void ValidateLogSchema(const dx::JSON &schema);

  /** Rsyslog head format: <pri>tag, where pri is a combination of facility and level
   *  Reture false if values of facility and level are not correct
   */
  bool formMessageHead(int facility, int level, const string &tag, string &head, string &errMsg);

  // Split a single long string into a vector of strings that are shorter than maxMsgSize
  void splitMessage(const string &msg, vector<string> &Msgs, int maxMsgSize);

  /** Send message to rsyslog
   *  Return true if succeeded; otherwise errMsg contains the detaialed information of error
   */  
  bool SendMessage2Rsyslog(int facility, int level, const string &tag, const string &msg, int maxMsgSize, string &errMsg);

  // store message to a local file (when it is not delivered
  void StoreMsgLocal(const string &filename, const string &msg);
};
#endif
