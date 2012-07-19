#include <gtest/gtest.h>
#include "dxLog.h"
#include "unixDGRAM.h"
#include "omp.h"

using namespace std;
using namespace DXLog;

const string socketPath = "test_unix_datagram_log";

class TestDGRAM : public UnixDGRAMReader{
  private:
    bool processMsg(){
      msgs.push_back(string(buffer));
      if (string(buffer).compare("Done") == 0) return true;
      return false;
    };

  public:
    vector<string> msgs;
    TestDGRAM() : UnixDGRAMReader(1000) { msgs.clear(); }
};

TEST(UNIXDGRAMTest, Invalid_Socket) {
  unlink(socketPath.c_str());
  string errMsg;
  ASSERT_FALSE(SendMessage2UnixDGRAMSocket(socketPath, "msg", errMsg));
  ASSERT_EQ(errMsg, "Error when sending log message: No such file or directory"); 
}

TEST(UNIXDGRAMTest, Integration) {
  unlink(socketPath.c_str());
  TestDGRAM test;
  string errMsg, errMsg2;

  omp_set_num_threads(2);
  bool ret_val[5];
  #pragma omp parallel sections
  {
    ret_val[0] = test.run(socketPath, errMsg);

    #pragma omp section
    {
      while(! test.isActive()) {
        usleep(100);
      }
      ret_val[1] = SendMessage2UnixDGRAMSocket(socketPath, "msg1", errMsg2);
      ret_val[2] = SendMessage2UnixDGRAMSocket(socketPath, "msg2", errMsg2);
      ret_val[3] = SendMessage2UnixDGRAMSocket(socketPath, "msg3", errMsg2);
      ret_val[4] = SendMessage2UnixDGRAMSocket(socketPath, "Done", errMsg2);
    }
  }
  ASSERT_FALSE(test.isActive());
  ASSERT_EQ(test.msgs.size(), 4);
  ASSERT_EQ(test.msgs[0], "msg1");
  ASSERT_EQ(test.msgs[1], "msg2");
  ASSERT_EQ(test.msgs[2], "msg3");
  ASSERT_EQ(test.msgs[3], "Done");

  for (int i = 0; i < 5; i ++)
    ASSERT_TRUE(ret_val[i]);

  ASSERT_FALSE(SendMessage2UnixDGRAMSocket(socketPath, "msg", errMsg));
  ASSERT_EQ(errMsg, "Error when sending log message: No such file or directory");

  unlink(socketPath.c_str());
}

TEST(UNIXDGRAMTest, Address_Used) {
  unlink(socketPath.c_str());
  TestDGRAM test1, test2;
  string errMsg, errMsg2, errMsg3;

  omp_set_num_threads(2);
  bool ret_val[3];
  #pragma omp parallel sections
  {
    ret_val[0] = test1.run(socketPath, errMsg);

    #pragma omp section
    {
      while (! test1.isActive()) {
        usleep(100);
      }
      ret_val[1] = test2.run(socketPath, errMsg2);
      ret_val[2] = SendMessage2UnixDGRAMSocket(socketPath, "Done", errMsg2);
    }
  }
  
  ASSERT_FALSE(test1.isActive());
  ASSERT_FALSE(test2.isActive());
  ASSERT_TRUE(ret_val[0]);
  ASSERT_FALSE(ret_val[1]);
  ASSERT_TRUE(ret_val[2]);

  ASSERT_EQ(errMsg2, "Socket error: Address already in use");
  ASSERT_EQ(test1.msgs.size(), 1);
  ASSERT_EQ(test1.msgs[0], "Done");
  ASSERT_EQ(test2.msgs.size(), 0);
  unlink(socketPath.c_str());
}

class InvalidLogInput {
  public:
    InvalidLogInput() {};

    dx::JSON GetOne(int k, string &errMsg) {
      dx::JSON data(dx::JSON_OBJECT);
      data["source"] = "DX_APP";

      switch(k) {
        case 0: 
          data = dx::JSON(dx::JSON_ARRAY);
          errMsg = "Log input, " + data.toString() + ", is not a JSON object";
          return data;
        case 1:
          data["timestamp"] = "2012-1-1";
          errMsg = "Log timestamp, " + data["timestamp"].toString() + ", is not an integer";
          return data;
        case 2:
          data.erase("source");
          errMsg = "Missing log source";
          return data;
        case 3:
          data["source"] = dx::JSON(dx::JSON_OBJECT);
          errMsg = "Log source, " + data["source"].toString() + ", is not a string";
          return data;
        case 4:
          data["source"] = "app";
          errMsg = "Invalid log source: app";
          return data;
        case 5:
          data["level"] = "x";
          errMsg = "Log level, " + data["level"].toString() + ", is not an integer";
          return data;
        case 6:
          data["level"] = 12;
          errMsg = "Invalid log level: 12";
          return data;
        case 7:
          data["hostname"] = 12;
          errMsg = "Log hostname, 12, is not a string";
          return data;
        default:
          return dx::JSON_NULL;
      }
    }

    int NumInput() { return 8; }
};

TEST(DXLOGTest, Invalid_Log_Input) {
  string errMsg1, errMsg2;
  InvalidLogInput input;

  for (int i = 0; i < input.NumInput(); i++) {
    dx::JSON data = input.GetOne(i, errMsg1);
    ASSERT_FALSE(ValidateLogData(data, errMsg2));
    ASSERT_EQ(errMsg1, errMsg2);
  }
};

class ValidLogInput {
  public:
    ValidLogInput() {};

    dx::JSON GetOne(int k, string &head) {
      dx::JSON data(dx::JSON_OBJECT);
      string errMsg;

      switch(k) {
        case 0:
          data["source"] = "DX_APP";
          head = "<14>DX_APP ";
          return data;
        case 1:
          data["level"] = 1;
          data["source"] = "DX_CM";
          head = "<9>DX_CM " ;
          return data;
        case 2:
          data["level"] = 4;
          data["source"] = "DX_EM";
          data["hostname"] = "localhost";
          data["timestamp"] = utcMS();
          head = "<12>DX_EM " ;
          return data;
        default:
          return dx::JSON_NULL;
      }
    }

    int NumInput() { return 3; }
};

TEST(DXLOGTest, Log_Input_Default_Value) {
  string errMsg;
  
  dx::JSON data(dx::JSON_OBJECT);
  data["source"] = "DX_H";

  ASSERT_TRUE(ValidateLogData(data, errMsg));
  
  ASSERT_EQ(errMsg, "");
  ASSERT_EQ(data["level"], 6);
  ASSERT_EQ(data["hostname"].type(), dx::JSON_STRING);
  ASSERT_EQ(data["timestamp"].type(), dx::JSON_INTEGER);
}

TEST(DXLOGTest, Rsyslog_Byte_Seq) {
  unlink(socketPath.c_str());
  TestDGRAM test;
  string errMsg, errMsg2;
  ValidLogInput input;
  int n = input.NumInput();

  omp_set_num_threads(2);
  vector<string> msgs1, msgs2;
  #pragma omp parallel sections
  {
    test.run(socketPath, errMsg);

    #pragma omp section
    {
      while(! test.isActive()) {
        usleep(100);
      }

      string msg, errMsg;
      for (int i = 0; i < n; i++) {
        dx::JSON data = input.GetOne(i, msg);
        msgs1.push_back(msg + data.toString());

        if (! ValidateLogData(data, errMsg)) throwString(errMsg);

        msgs2.push_back(msg + data.toString());
        SendMessage2Rsyslog(int(data["level"]), data["source"].get<string>(), data.toString(), errMsg2, socketPath);
      }
      SendMessage2UnixDGRAMSocket(socketPath, "Done", errMsg2);
    }
  }
  ASSERT_FALSE(test.isActive());
  ASSERT_EQ(test.msgs.size(), n+1);
  for (int i = 0; i < n-1; i++)
    ASSERT_NE(test.msgs[i], msgs1[i]);
  ASSERT_EQ(test.msgs[n-1], msgs1[n-1]);

  for (int i = 0; i < n; i++)
    ASSERT_EQ(test.msgs[i], msgs2[i]);
  ASSERT_EQ(test.msgs[n], "Done");

  unlink(socketPath.c_str());
}

bool verify_mongodb_schema(const dx::JSON &schema, string &errMsg) {
  try{
    ValidateDBSchema(schema);
    return true;
  } catch (const string &msg) {
    errMsg = msg;
    return false;
  }
}

TEST(DXLogTest, MongoDB_Schema) {
  string errMsg;
  
  dx::JSON schema(dx::JSON_ARRAY);
  ASSERT_FALSE(verify_mongodb_schema(schema, errMsg));
  ASSERT_EQ(errMsg, "Mongodb schema, " + schema.toString() + ", is not a JSON object");

  schema = dx::JSON(dx::JSON_OBJECT);
  schema["DX_H"] = dx::JSON(dx::JSON_ARRAY);
  ASSERT_FALSE(verify_mongodb_schema(schema, errMsg));
  ASSERT_EQ(errMsg, "DX_H mongodb schema, " + schema["DX_H"].toString() + ", is not a JSON object");

  schema.erase("DX_H");
  schema["DX_H"] = dx::JSON(dx::JSON_OBJECT);
  ASSERT_FALSE(verify_mongodb_schema(schema, errMsg));
  ASSERT_EQ(errMsg, "DX_H: missing collection");

  schema["DX_H"]["collection"] = "h";
  ASSERT_TRUE(verify_mongodb_schema(schema, errMsg));
}

TEST(DXLOGTest, logger) {
  logger log;

  string errMsg1, errMsg2;
  InvalidLogInput input;
  
  for (int i = 0; i < input.NumInput(); i++) {
    dx::JSON data = input.GetOne(i, errMsg1);
    ASSERT_FALSE(log.Log(data, errMsg2));
    ASSERT_EQ(errMsg1, errMsg2);
  }

  unlink(socketPath.c_str());
  TestDGRAM test;
  vector<string> msgs;

  ValidLogInput input2;
  int n = input2.NumInput();

  omp_set_num_threads(2);
  #pragma omp parallel sections
  {
    test.run(socketPath, errMsg1);

    #pragma omp section
    {
      while(! test.isActive()) {
        usleep(100);
      }

      string msg, errMsg;
      for (int i = 0; i < n; i++) {
        dx::JSON data = input2.GetOne(i, msg);
        msgs.push_back(msg + data.toString());
        if (! log.Log(data, errMsg1, socketPath)) throwString(errMsg1);
      }
      SendMessage2UnixDGRAMSocket(socketPath, "Done", errMsg2);
    }
  }
  ASSERT_FALSE(test.isActive());
  ASSERT_EQ(test.msgs.size(), n+1);
  for (int i = 0; i < n-1; i++)
    ASSERT_NE(test.msgs[i], msgs[i]);
  ASSERT_EQ(test.msgs[n-1], msgs[n-1]);
  ASSERT_EQ(test.msgs[n], "Done");
  
  unlink(socketPath.c_str());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
