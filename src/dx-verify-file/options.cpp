// Copyright (C) 2013-2014 DNAnexus, Inc.
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

#include "options.h"

using namespace std;

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <boost/filesystem/operations.hpp>
#include "log.h"
#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#ifdef STATIC_BUILD
  #ifdef MAC_BUILD
    #include <mach-o/dyld.h>
  #endif
#endif

namespace fs = boost::filesystem;

Options::Options() {
  int defaultMD5threads = int(boost::thread::hardware_concurrency()) - 1;
  if (defaultMD5threads < 1) {
    defaultMD5threads = 1;
  }

  visible_opts = new po::options_description("Available options");
  visible_opts->add_options()
    ("help,h", "Produce a help message")
    ("version", "Print the version")
    ("env,e", "Print environment information")
    ("auth-token,a", po::value<string>(&authToken), "Specify the authentication token")
    ("remote-file,r", po::value<vector<string> >(&remoteFiles), "ID of the remote file")
    ("local-file,l", po::value<vector<string> >(&localFiles), "Local file path")
    ("read-threads", po::value<int>(&readThreads)->default_value(1), "Number of parallel disk read threads")
    ("md5-threads", po::value<int>(&md5Threads)->default_value(defaultMD5threads), "Number of parallel MD5 compute threads")
    ("verbose,v", po::bool_switch(&verbose), "Verbose logging")
    ;

  hidden_opts = new po::options_description();
  hidden_opts->add_options()
    ("apiserver-protocol", po::value<string>(&apiserverProtocol), "API server protocol")
    ("apiserver-host", po::value<string>(&apiserverHost), "API server host")
    ("apiserver-port", po::value<int>(&apiserverPort)->default_value(-1), "API server port")
    ("certificate-file", po::value<string>(&certificateFile)->default_value(""), "Certificate file (for verifying peer). Set to NOVERIFY for no check.");
    ;

  command_line_opts = new po::options_description();
  command_line_opts->add(*visible_opts);
  command_line_opts->add(*hidden_opts);
}

// This function is responsible for:
//  - If --auth-token, --apiserver-* params are not provided, then set it from
//    values in dxcpp (dx::config::*)
//  - If --auth-token, --apiserver-* params are provided, then set the values
//    in dxcpp (dx::config::*), so that dxcpp uses correct host, token, etc
//  - Throw error if a required parameter is not set anywhere (provided on command line, or in dx::config)
void Options::setApiserverDxConfig() {
  using namespace dx::config; // for SECURITY_CONTEXT(), APISERVER_*(), etc

  if (authToken.empty()) {
    // If --auth-token flag is not used, check that dx::config::SECURITY_CONTEXT() has a auth token, else throw
    if (SECURITY_CONTEXT().size() == 0) 
      throw runtime_error("No Authentication token found, please provide a correct auth token (use --auth-token or dx login)");
  } else {
    LOG << "Setting dx::config::SECURITY_CONTEXT() from value provided at run time: '" << authToken << "'" << endl;
    SECURITY_CONTEXT() = dx::JSON::parse("{\"auth_token_type\": \"Bearer\", \"auth_token\": \"" + authToken + "\"}");
  }

  if (!apiserverProtocol.empty()) {
    LOG << "Setting dx::config::APISERVER_PROTOCOL from value provided at run time: '" << apiserverProtocol << "'" << endl;
    APISERVER_PROTOCOL() = apiserverProtocol;
  }
  if (apiserverPort != -1) {
    LOG << "Setting dx::config::APISERVER_PORT from value provided at run time: '" << apiserverPort << "'" << endl;
    APISERVER_PORT() = boost::lexical_cast<string>(apiserverPort);
  }
  if (!apiserverHost.empty()) {
    LOG << "Setting dx::config::APISERVER_HOST from value provided at run time: '" << apiserverHost << "'" << endl;
    APISERVER_HOST() = apiserverHost;
  }
  // Now check that dxcpp has all of the required apiserver params set
  if (APISERVER().empty()) {
    throw runtime_error("At least one of apiserver host/port/protocol is not specified, unable to continue without this information."
                        "Please use --apiserver-host, --apiserver-port, --apiserver-protocol to provide this info on command line");
  }
}

void Options::parse(int argc, char * argv[]) {
  po::store(po::command_line_parser(argc, argv).options(*command_line_opts).run(), vm);
  po::notify(vm);
  Log::enabled = verbose;
}

bool Options::help() {
  return vm.count("help");
}

bool Options::version() {
  return vm.count("version");
}

bool Options::env() {
  return vm.count("env");
}

void Options::printHelp(char * programName) {
  cerr << "Usage: " << programName << " [options] -r <remote_file1_id> -l <local_file1> [-r <remote_file2_id> -l <local_file2> ...]" << endl
       << endl
       << (*visible_opts) << endl;
}

#ifdef STATIC_BUILD
  #ifdef MAC_BUILD
  // Returns path of executable on Mac (not portable)
  string getExecutablePathOnMac() {
    char path[1024 * 100];
    uint32_t size = sizeof(path);
    if (!_NSGetExecutablePath(path, &size) == 0)
        throw runtime_error(" _NSGetExecutablePath() returned non-zero exit code. Unexpected.");
    // now resolve any symlinks
    // https://developer.apple.com/library/mac/#documentation/Darwin/Reference/ManPages/man3/realpath.3.html
    char *resolved_path = realpath(path, NULL);
    if (resolved_path == NULL)
      throw runtime_error("realpath() returned NULL pointer. Unexpected.");
    string toStr = resolved_path;
    free(resolved_path); // memory was allocated by realpath()
    return fs::path(toStr).remove_filename().string(); // return just the directory path
  }
  #endif
#endif

#ifdef STATIC_BUILD
  // Looks at either the 'certificate-file' flag's value,
  // or tries to find the certificate file in a few known
  // standard locations. Throws an error if not found anywhere.
  // Note: Do not call when protocol being used != https
  //
  // Only relevant for static linking (in case of dynamic linking, the library will know the location itself)
  void setCertificateFile(const string &certificateFile) {
    #ifdef MAC_BUILD
      const unsigned ARR_SIZE = 3;
    #else
      const unsigned ARR_SIZE = 2;
    #endif
    const char *standardPathLocations[ARR_SIZE]= {
      "/etc/ssl/certs/ca-certificates.crt", // default on ubuntu
      "/etc/pki/tls/certs/ca-bundle.crt" // default on centos
    };
    #ifdef MAC_BUILD
      // If we are building on mac, then add one more path to look for certificate file, i.e.,
      // the current executable path (since we bundle certificate file together with distribution)
      string certpath = getExecutablePathOnMac() + "/ca-certificates.crt";
      standardPathLocations[ARR_SIZE - 1] = certpath.c_str();
    #endif
    if (!certificateFile.empty()) {
      LOG << "Option '--certificate-file' present, and value is: '" << certificateFile << "'" << endl;
      dx::config::CA_CERT() = certificateFile;
      return;
    } else {
      if (dx::config::CA_CERT().empty()) {
        LOG << "--certificate-file is not specified, and env var 'DX_CA_CERT' is not present either.\n";
        #ifdef WINDOWS_BUILD
          LOG << " For Windows version, we don't look for CA certificate in standard location, but rather use the curl default." << endl;
          return;
        #else
          LOG << " Will look in standard locations for certificate file (to verify peers)" << endl;
          // Look into standard locations
          for (unsigned i = 0; i < ARR_SIZE; ++i) {
            LOG << "\tChecking in location: '" << standardPathLocations[i] << "'";
            fs::path p (standardPathLocations[i]);
            if (fs::exists(p)) {
              LOG << " ... Found! Will use it." << endl;
              dx::config::CA_CERT() = standardPathLocations[i];
              return;
            }
            LOG << " ... not found." << endl;
          }
          // If we are here, we haven't found certificate file in any of the standard locations. Throw error
          throw runtime_error("Unable to find certificate file (for verifying authenticity of the peer over SSL connection) in any of the standard locations.\n"
                              "Please use the undocumented option: '--certificate-file' to specify it's location, or set it to string 'NOVERIFY' for disabling "
                              "authenticity check of the remote host (not recommended).");
        #endif
      } else {
        // use the DX_CA_CERT value (already set by dxcpp's static initializer).
        LOG << "'--certificate-file' option is absent, but 'DX_CA_CERT' is present, value is: '" << dx::config::CA_CERT() << "'. Will use it." << endl;
        return;
      }
    }
  }
#endif

void Options::validate() {
  if (localFiles.size() != remoteFiles.size())
    throw runtime_error("Equal number of local files & remote file IDs must be specified");
  
  if (localFiles.size() < 1)
    throw runtime_error("At least one pair of local/remote file must be specified");
  
  // Check that all local files actually exist
  // - Resolve all symlinks
  for (unsigned i = 0; i < localFiles.size(); ++i) {
    fs::path p (localFiles[i]);
    if (!fs::exists(p)) {
      throw runtime_error("File \"" + localFiles[i] + "\" does not exist");
    }
    if (fs::is_symlink(p)) {
      localFiles[i] = fs::read_symlink(p).string(); 
    }
  }

// Taken care by setApiserverDxConfig()
/*
  if (authToken.empty()) {
    throw runtime_error("An authentication token must be provided");
  }
  if (apiserverProtocol.empty()) {
    throw runtime_error("API server protocol must be specified (\"http\" or \"https\")");
  }
  if (apiserverHost.empty()) {
    throw runtime_error("An API server must be specified");
  }
  if (apiserverPort < 1) {
    ostringstream msg;
    msg << "Invalid API server port: " << apiserverPort;
    throw runtime_error(msg.str());
  }
 */ 
  #ifdef STATIC_BUILD
  // ugly way to do case insensitive comparison, but works
  // without adding additional dependencies, like boost string, etc
  string lowerCaseApiserverProtocol = "";
  for (unsigned i = 0; i < apiserverProtocol.length(); ++i)
    lowerCaseApiserverProtocol += tolower(apiserverProtocol[i]);
  
  if (lowerCaseApiserverProtocol == "https") {
    setCertificateFile(certificateFile);
  }
  #endif
  
  if (readThreads < 1) {
    ostringstream msg;
    msg << "Number of read threads must be positive: " << readThreads;
    throw runtime_error(msg.str());
  }
  if (md5Threads < 1) {
    ostringstream msg;
    msg << "Number of MD5 compute threads must be positive: " << md5Threads;
    throw runtime_error(msg.str());
  }
}

ostream &operator<<(ostream &out, const Options &opt) {
  if (opt.vm.count("help")) {
    out << (*(opt.visible_opts)) << endl;
  } else {
    out << "Options:" << endl
        << "  auth token: " << opt.authToken << endl
        << "  API server protocol: " << opt.apiserverProtocol << endl
        << "  API server host: " << opt.apiserverHost << endl
        << "  API server port: " << opt.apiserverPort << endl;
    
    out << "  local files:";
    for (unsigned int i = 0; i < opt.localFiles.size(); ++i)
      out << " \"" << opt.localFiles[i] << "\"";
    out << endl;
    
    out << "  remote files:";
    for (unsigned int i = 0; i < opt.remoteFiles.size(); ++i)
      out << " \"" << opt.remoteFiles[i] << "\"";
    out << endl;


    out << "  read threads: " << opt.readThreads << endl
        << "  md5 threads: " << opt.md5Threads << endl
        << "  verbose: " << opt.verbose << endl
      ;
  }
  return out;
}
