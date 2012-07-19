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

TEST(DXLOGTest, Invalid_Log_Input) {
  string errMsg;
  
  dx::JSON data(dx::JSON_ARRAY);
  ASSERT_FALSE(ValidateLogData(data, errMsg));
  ASSERT_EQ(errMsg, "Log input, " + data.toString() + ", is not a JSON object");

  data = dx::JSON(dx::JSON_OBJECT);
  
  data["timestamp"] = "2012-1-1";
  ASSERT_FALSE(ValidateLogData(data, errMsg));
  ASSERT_EQ(errMsg, "Log timestamp, " + data["timestamp"].toString() + ", is not an integer");

  data["timestamp"] = utcMS();
  ASSERT_FALSE(ValidateLogData(data, errMsg));
  ASSERT_EQ(errMsg, "Missing log source");

  data["source"] = dx::JSON(dx::JSON_OBJECT);
  ASSERT_FALSE(ValidateLogData(data, errMsg));
  ASSERT_EQ(errMsg, "Log source, " + data["source"].toString() + ", is not a string");

  data["source"] = "app";
  ASSERT_FALSE(ValidateLogData(data, errMsg));
  ASSERT_EQ(errMsg, "Invalid log source: app");
 
  data["source"] = "DX_APP";
  data["level"] = "x";
  ASSERT_FALSE(ValidateLogData(data, errMsg));
  ASSERT_EQ(errMsg, "Log level, " + data["level"].toString() + ", is not an integer");

  data["level"] = 4;
  data["hostname"] = 12;
  ASSERT_FALSE(ValidateLogData(data, errMsg));
  ASSERT_EQ(errMsg, "Log hostname, 12, is not a string");
}

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

  omp_set_num_threads(2);
  bool ret_val[4];
  #pragma omp parallel sections
  {
    ret_val[0] = test.run(socketPath, errMsg);

    #pragma omp section
    {
      while(! test.isActive()) {
        usleep(100);
      }
      ret_val[1] = SendMessage2Rsyslog(6, "DX_APP", "msg1", errMsg2, socketPath);
      ret_val[2] = SendMessage2Rsyslog(1, "DX_CM", "{\"t\":\"msg2\"}", errMsg2, socketPath);
      ret_val[3] = SendMessage2UnixDGRAMSocket(socketPath, "Done", errMsg2);
    }
  }
  ASSERT_FALSE(test.isActive());
  ASSERT_EQ(test.msgs.size(), 3);
  ASSERT_EQ(test.msgs[0], "<14>DX_APP msg1");
  ASSERT_EQ(test.msgs[1], "<9>DX_CM {\"t\":\"msg2\"}");
  ASSERT_EQ(test.msgs[2], "Done");

  for (int i = 0; i < 4; i ++)
    ASSERT_TRUE(ret_val[i]);

  unlink(socketPath.c_str());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
