// Copyright (C) 2013 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

#include <gtest/gtest.h>
#include "SimpleHttp.h"

using namespace std;

bool exceptionflag;
// A macro which asserts that a JSONException is thrown by provided statement
#define ASSERT_HTTPEXCEPTION(a) \
  exceptionflag = false; \
  try { a; ASSERT_TRUE(false); } catch(HttpRequestException &e) { exceptionflag =  true; } \
  ASSERT_TRUE(exceptionflag);

TEST(HttpRequestTest, Test_HTTP_GET) {
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

TEST(HttpRequestTest, Test_HTTP_HEAD) {
  HttpRequest hr = HttpRequest::request(HTTP_HEAD, "http://www.google.com");
  
  ASSERT_EQ(hr.respData.length(), 0u);
  ASSERT_EQ(hr.responseCode, 200);
  const HttpHeaders h = hr.getRespHeaders();
  // Date header is present in google's page
  ASSERT_TRUE(h.isPresent("Date"));
}

TEST(HttpRequestTest, Test_HTTP_HEAD_AND_GET) {
  HttpRequest hr = HttpRequest::request(HTTP_HEAD, "http://www.google.com");
  
  ASSERT_EQ(hr.respData.length(), 0u);
  ASSERT_EQ(hr.responseCode, 200);
  const HttpHeaders h = hr.getRespHeaders();
  // Date header is present in google's page
  ASSERT_TRUE(h.isPresent("Date"));

  hr.buildRequest(HTTP_GET, "http://www.google.com");
  hr.send();
  ASSERT_EQ(hr.responseCode, 200);
  // "<html" is present in google homepage
  ASSERT_NE(hr.respData.find("<html"), string::npos);
  const HttpHeaders h2 = hr.getRespHeaders();
  // Date header is present in google's page
  ASSERT_TRUE(h2.isPresent("Date"));
}

TEST(HttpRequestTest, Test_HTTPS_HEAD_AND_GET) {
  HttpRequest hr = HttpRequest::request(HTTP_HEAD, "https://www.google.com");
  
  ASSERT_EQ(hr.respData.length(), 0u);
  ASSERT_EQ(hr.responseCode, 200);
  const HttpHeaders h = hr.getRespHeaders();
  // Date header is present in google's page
  ASSERT_TRUE(h.isPresent("Date"));

  hr.buildRequest(HTTP_GET, "https://www.google.com");
  hr.send();
  ASSERT_EQ(hr.responseCode, 200);
  // "<html" is present in google homepage
  ASSERT_NE(hr.respData.find("<html"), string::npos);
  const HttpHeaders h2 = hr.getRespHeaders();
  // Date header is present in google's page
  ASSERT_TRUE(h2.isPresent("Date"));
}


int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
      return RUN_ALL_TESTS();
}
