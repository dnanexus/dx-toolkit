#include <gtest/gtest.h>
#include "SimpleHttp.h"

using namespace std;

bool exceptionflag;
// A macro which asserts that a JSONException is thrown by provided statement
#define ASSERT_HTTPEXCEPTION(a) \
  exceptionflag = false; \
  try { a; ASSERT_TRUE(false); } catch(HttpRequestException &e) { exceptionflag =  true; } \
  ASSERT_TRUE(exceptionflag);

TEST(JSONTest, Test_HTTP_GET) {
  HttpRequest hr;
  hr.buildRequest(HTTP_GET, "http://www.google.com");
  hr.send();

  ASSERT_EQ(hr.responseCode, 200);
  // "<html" is present in google homepage
  ASSERT_NE(hr.respData.find("<html"), string::npos);
  const HttpHeaders h = hr.getRespHeaders();
  // Date header is present in google's page
  ASSERT_TRUE(h.isPresent("Date"));
}

TEST(JSONTest, Test_HTTP_HEAD) {
  HttpRequest hr = HttpRequest::request(HTTP_HEAD, "http://www.google.com");
  
  ASSERT_EQ(hr.respData.length(), 0u);
  ASSERT_EQ(hr.responseCode, 200);
  const HttpHeaders h = hr.getRespHeaders();
  // Date header is present in google's page
  ASSERT_TRUE(h.isPresent("Date"));
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
      return RUN_ALL_TESTS();
}
