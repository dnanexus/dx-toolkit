// Copyright (C) 2016 DNAnexus, Inc.
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



// This function should be called before opt.setApiserverDxConfig() is called,
// since opt::setApiserverDxConfig() changes the value of dx::config::*, based on command line args

#include <string>
#include <time.h>
#include "ua_test.h"
#include "dxcpp/dxcpp.h"
#include "dxjson/dxjson.h"
#include "api_helper.h"
#include "options.h"
#include "api.h"
#include "round_robin_dns.h"

#if WINDOWS_BUILD
#include <windows.h>
#else
#include <sys/utsname.h>
#endif

using namespace std;
using namespace dx;
using namespace dx::config;

void runTests()
{
  version();
  printEnvironmentInfo(false);
  testSystemGreet();
  testWhoAmI();
  currentProject();
  proxySettings();
  osInfo();
  certificateFile();
  resolveAmazonS3();
  contactGoogle();
}

void version() {
  cout << "Upload Agent Version: " << UAVERSION;
#if OLD_KERNEL_SUPPORT
  cout << " (old-kernel-support)";
#endif
  cout << endl
       << "  git version: " << DXTOOLKIT_GITVERSION << endl
       << "  libboost version: " << (BOOST_VERSION / 100000) << "." << ((BOOST_VERSION / 100) % 1000) << "." << (BOOST_VERSION % 100) << endl
       << "  libcurl version: " << LIBCURL_VERSION_MAJOR << "." << LIBCURL_VERSION_MINOR << "." << LIBCURL_VERSION_PATCH << endl;
}

void osInfo(){
#if WINDOWS_BUILD
  OSVERSIONINFO vi;
  vi.dwOSVersionInfoSize = sizeof(vi);
  try {
    GetVersionEx(&vi);
    cout << "Operating System:" << endl
         << "  Windows: " << vi.dwMajorVersion << "." << vi.dwMinorVersion << "." << vi.dwBuildNumber
         << "." << vi.dwPlatformId << " " << vi.szCSDVersion << endl;
  } catch(exception &e){
    cout << "Unable to get OS information" << e.what() << endl;
  }
#else
  struct utsname uts;
  uname(&uts);
  cout << "Operating System:" << endl
       << "  Name:    " << uts.sysname << endl
       << "  Release: " << uts.release << endl
       << "  Version: " << uts.version << endl
       << "  Machine: " << uts.machine << endl;
#endif
}

void printEnvironmentInfo(bool printToken) {
  cout << "Upload Agent v" << UAVERSION << ", environment info:" << endl
       << "  API server protocol: " << APISERVER_PROTOCOL() << endl
       << "  API server host:     " << APISERVER_HOST() << endl
       << "  API server port:     " << APISERVER_PORT() << endl;

  if (printToken) {
    if (SECURITY_CONTEXT().size() != 0)
      cout << "  Auth token: " << SECURITY_CONTEXT()["auth_token"].get<string>() << endl;
    else
      cout << "  Auth token: " << endl;
  }
}

void currentProject() {
  string projID = CURRENT_PROJECT();
  try {
    if (projID.empty()) {
      cout << "Current Project: None" << endl;
    } else {
      string projName = getProjectName(projID);
      cout << "Current Project: " << projName << " (" << projID << ")" << endl;
    }
  } catch (DXAPIError &e) {
    cout << "Current Project: "<< " (" << projID << ")" << e.what() << endl;
  }
}

bool getProxyValue(const char * name, string &value) {
  if (getenv(name) == NULL)
    return false;
  value = string(getenv(name));
  // Remove credentials from string
  std::size_t atSimbol = value.find_first_of("@");
  if (atSimbol != string::npos ) {
    if (value.substr(0, 7) == "http://") {
      value.replace(7, atSimbol-7, "****");
    } else if (value.substr(0, 8) == "https://") {
      value.replace(8, atSimbol-8, "****");
    } else {
      value.replace(0, atSimbol, "****");
    }
    cout << "  To see actual username and password run: echo $" << name << endl;
    cout << "  Note that special characters in username / password might prevent credentials from being resolved properly." << endl;
  }
  return true;
}


void proxySettings() {
  string value;
  cout << "Proxy Settings:" << endl;
  bool proxySet = false;
  if (getProxyValue("http_proxy", value))  { cout << "  http_proxy: " << value << endl; proxySet = true;}
  if (getProxyValue("https_proxy", value)) { cout << "  https_proxy: " << value << endl;proxySet = true;}
  if (getProxyValue("HTTP_PROXY", value))  { cout << "  HTTP_PROXY: " << value << endl;proxySet = true;}
  if (getProxyValue("HTTPS_PROXY", value)) { cout << "  HTTP_PROXY: " << value << endl;proxySet = true;}
  if (!proxySet) { cout << "  No proxy set in environment." << endl; }
}

void certificateFile() {
  cout << "CA Certificate: " << CA_CERT() << endl;
}

void testWhoAmI(){
  cout << "Current User: ";
  try {
    JSON res = systemWhoami(string("{}"), false);
    cout  << res["id"].get<string>() << endl;
  } catch(DXAPIError &e) {
    cout << "Error contacting the api: " << e.what() << endl;
  } catch (DXConnectionError &e) {
    cout << "Error contacting the api: " << e.what() << endl;
  } catch (JSONException &e) {
    cout << "Error contacting the api: " << e.what() << endl;
  } catch (...) {
    cout << "Error contacting the api." << endl;
  }
}

void testSystemGreet() {
  try {
    JSON inp = getPlatformInputHash();
    JSON res = systemGreet(inp, false);
    if (res["update"]["available"].get<bool>() == false) {
      cout << "Your copy of Upload Agent is up to date." << endl;
    } else {
      string ver = res["update"]["version"].get<string>();
      cout << "A new version is available: " << ver << endl;
    }

    const JSON &messageArray = res["messages"];

    cout << "System Messages:" << endl << endl;
    if (messageArray.size() == 0) {
      cout << "  There are currently no system messages." << endl;
    } else {
      for (JSON::const_array_iterator it = messageArray.array_begin(); it < messageArray.array_end(); ++it) {
        const JSON &message = *it;
        const string title = message["title"].get<string>();
        const string body = message["body"].get<string>();
        const time_t date = message["date"].get<time_t>();
        cout << "Date: " << ctime(&date);
        cout << "Subject: " << title << endl;
        cout << body << endl << endl;
      }
    }
  } catch(DXAPIError &e) {
    cout << "Error contacting the api: " << e.what() << endl;
  } catch (DXConnectionError &e) {
    cout << "Error contacting the api: " << e.what() << endl;
  } catch (JSONException &e) {
    cout << "Error contacting the api: " << e.what() << endl;
  } catch (...) {
    cout << "Error contacting the api." << endl;
  }
}

void contactGoogle() {
  cout << "Testing connection:" << endl;
  try {
    string url = "http://www.google.com/";
    HttpRequest req = HttpRequest::request(dx::HTTP_GET, url);
    if (req.responseCode == 200 || req.responseCode == 302) {
      cout << "  Successfully contacted google.com over http: (" << req.responseCode << ")" << endl;
    } else {
      cout << "  Unable to contact google.com over http: (" << req.responseCode << ")" << endl;
    }
  } catch (HttpRequestException &e) {
    cout << "Error contacting google over http: " << e.what();
  } catch (...) {
    cout << "Error contacting the api." << endl;
  }

  try {
    string url = "https://www.google.com/";
    HttpRequest req = HttpRequest::request(dx::HTTP_GET, url);
    if (req.responseCode == 200 || req.responseCode == 302) {
      cout << "  Successfully contacted google.com over https: (" << req.responseCode << ")" << endl;
    } else {
      cout << "  Unable to contact google.com over https: (" << req.responseCode << ")" << endl;
    }
  } catch (HttpRequestException &e) {
    cout << "Error contacting google over https: " << e.what() << endl;
  } catch (...) {
    cout << "Error contacting google" << endl;
  }
}

void resolveAmazonS3(){
  cout << "Resolving Amazon S3:" << endl;
  string awsIP = getRandomIP("s3.amazonaws.com");
  if (awsIP.empty()){
    cout << "  Unable to resolve Amazon S3" << endl;
  } else {
    cout << "  Resolved to " << awsIP << endl;
  }
}

