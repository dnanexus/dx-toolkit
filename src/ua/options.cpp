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

#include <cmath>
#include <limits>

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/regex.hpp>

#include "dxcpp/dxlog.h"
#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#if MAC_BUILD
#include <mach-o/dyld.h>
#endif

namespace fs = boost::filesystem;

using namespace std;
using namespace dx;

#if WINDOWS_BUILD
const int64_t DEFAULT_CHUNK_SIZE = 30 * 1024 * 1024;
const char * DEFAULT_RAW_CHUNK_SIZE = "30M";
const int DEFAULT_UPLOAD_THREADS = 6;
#else
const int64_t DEFAULT_CHUNK_SIZE = 75 * 1024 * 1024;
const char * DEFAULT_RAW_CHUNK_SIZE = "75M";
const int DEFAULT_UPLOAD_THREADS = 8;
#endif

const int DEFAULT_READ_THREADS = 2;

Options::Options() {
  int defaultCompressThreads = std::min(8, std::max(int(boost::thread::hardware_concurrency()) - 1, 1)); //don't use more than 8 cores for compression (by default)

  vector<string> defaultFolders;
  defaultFolders.push_back("/");

  visible_opts = new po::options_description("Available options");
  visible_opts->add_options()
    ("help,h", "Produce a help message")
    ("version", "Print the version")
    ("env,e", "Print environment information")
    ("auth-token,a", po::value<string>(&authToken), "Specify the authentication token")
    ("project,p", po::value<vector<string> >(&projects), "Name or ID of the destination project")
    ("folder,f", po::value<vector<string> >(&folders)->default_value(defaultFolders, "/"), "Name of the destination folder")
    ("name,n", po::value<vector<string> >(&names), "Name of the remote file (Note: Extension \".gz\" will be appended if the file is compressed before uploading)")
    ("read-threads", po::value<int>(&readThreads)->default_value(DEFAULT_READ_THREADS), "Number of parallel disk read threads")
    ("compress-threads,c", po::value<int>(&compressThreads)->default_value(defaultCompressThreads), "Number of parallel compression threads")
    ("upload-threads,u", po::value<int>(&uploadThreads)->default_value(DEFAULT_UPLOAD_THREADS), "Number of parallel upload threads")
    ("chunk-size,s", po::value<string>(&rawChunkSize)->default_value(DEFAULT_RAW_CHUNK_SIZE), "Size of chunks in which the file should be uploaded. Specify an integer size in bytes or append optional units (B, K, M, G). E.g., '50M' sets chunk size to 50 megabytes.")
    ("throttle", po::value<string>(&rawThrottle), "Limit maximum upload speed. Specify an integer to set speed in bytes/second or append optional units (B, K, M, G). E.g., '3M' limits upload speed to 3 megabytes/second. If not set, uploads are not throttled.")
    ("tries,r", po::value<int>(&tries)->default_value(3), "Number of tries to upload each chunk")
    ("do-not-compress", po::bool_switch(&doNotCompress), "Do not compress file(s) before upload")
    ("progress,g", po::bool_switch(&progress), "Report upload progress")
    ("verbose,v", po::bool_switch(&verbose), "Verbose logging")
    ("wait-on-close", po::bool_switch(&waitOnClose), "Wait for file objects to be closed before exiting")
    ("do-not-resume", po::bool_switch(&doNotResume), "Do not attempt to resume any incomplete uploads")
    ;

  hidden_opts = new po::options_description();
  hidden_opts->add_options()
    ("file", po::value<vector<string> >(&files), "File to upload")
    ("apiserver-protocol", po::value<string>(&apiserverProtocol), "API server protocol")
    ("apiserver-host", po::value<string>(&apiserverHost), "API server host")
    ("apiserver-port", po::value<int>(&apiserverPort)->default_value(-1), "API server port")
    ("certificate-file", po::value<string>(&certificateFile)->default_value(""), "Certificate file (for verifying peer). Set to NOVERIFY for no check.")
    ("no-round-robin-dns", po::bool_switch(&noRoundRobinDNS), "Disable explicit resolution of ip address by /UPLOAD calls (for round robin DNS)")
    // Options for running import apps
    ("reads", po::bool_switch(&reads), "After uploading is complete, run import app to convert file(s) to Reads object(s)")
    ("paired-reads", po::bool_switch(&pairedReads), "Same as --reads option, but assumes file sequence to be pairs of left, and right reads (e.g., L1 R1 L2 R2 L3 R3 ...)")
    ("mappings", po::bool_switch(&mappings), "After uploading is complete, run import app to convert file(s) to Mappings object(s)")
    ("variants", po::bool_switch(&variants), "After uploading is complete, run import app to convert file(s) to Variants object(s)")
    ("ref-genome", po::value<string>(&refGenome), "ID or name of the reference genome (must be present if and only if --mappings, or, --variants flag is used)")
    ;

  command_line_opts = new po::options_description();
  command_line_opts->add(*visible_opts);
  command_line_opts->add(*hidden_opts);

  pos_opts = new po::positional_options_description();
  pos_opts->add("file", -1);
}

size_t parseSize(const string &sizeStr) {
  static const boost::regex sizeExpr("(\\d+)([BKMG]?)");
  boost::smatch match;

  if (regex_match(sizeStr, match, sizeExpr)) {
    assert(match.size() == 3);

    size_t size = boost::lexical_cast<size_t>(match[1]);
    string units = match[2];
    size_t sizeBytes;

    if ((units == "B") || (units == "")) {
      // bytes
      sizeBytes = size;
    } else if (units == "K") {
      // kilobytes
      sizeBytes = size * 1024;
    } else if (units == "M") {
      // megabytes
      sizeBytes = size * 1024 * 1024;
    } else if (units == "G") {
      // gigabytes
      sizeBytes = size * 1024 * 1024 * 1024;
    } else {
      // can't happen unless regex gets out of sync with this conditional
      throw runtime_error("Unrecognized units: '" + units + "'");
    }
    return sizeBytes;
  } else {
    throw runtime_error("Invalid size argument: '" + sizeStr + "'; provide an integer optionally followed by units (B, K, M, or G)");
  }
}

void Options::parse(int argc, char * argv[]) {
  po::store(po::command_line_parser(argc, argv).options(*command_line_opts).positional(*pos_opts).run(), vm);
  po::notify(vm);
  dx::Log::ReportingLevel() = (verbose) ? dx::logDEBUG4 : dx::DISABLE_LOGGING;

  if (rawChunkSize.empty()) {
    chunkSize = DEFAULT_CHUNK_SIZE;
    DXLOG(logINFO) << "Using default chunk size." << endl;
  } else {
    chunkSize = parseSize(rawChunkSize);
    DXLOG(logINFO) << "Setting chunk size to " << chunkSize << " bytes." << endl;
  }

  if (rawThrottle.empty()) {
    throttle = -1;
    DXLOG(logINFO) << "Throttling is disabled." << endl;
  } else {
    throttle = parseSize(rawThrottle);
    DXLOG(logINFO) << "Throttling is enabled. Maximum upload speed is set to " << throttle << " bytes/second." << endl;

    int oldUploadThreads = uploadThreads;
    uploadThreads = min(uploadThreads, static_cast<int>(ceil(throttle / (1024.0 * 1024.0) + numeric_limits<double>::epsilon())));
    if (uploadThreads != oldUploadThreads) {
      DXLOG(logINFO) << "Adjusting number of upload threads from " << oldUploadThreads << " to " << uploadThreads << "." << endl;
    } else {
      DXLOG(logINFO) << "Number of upload threads is " << uploadThreads << "." << endl;
    }
  }
}

#if MAC_BUILD
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

/*
 * Looks at the --certificate-file flag's value, or tries to find the
 * certificate file in a few known standard locations. Throws an error if
 * it is not found anywhere. Note: do not call when the protocol being used
 * is not https.
 */
void setCertificateFile(const string &certificateFile) {
  using namespace dx::config;
  #if MAC_BUILD
    const unsigned ARR_SIZE = 3;
  #else
    const unsigned ARR_SIZE = 2;
  #endif
  const char *standardPathLocations[ARR_SIZE]= {
    "/etc/ssl/certs/ca-certificates.crt", // default on ubuntu
    "/etc/pki/tls/certs/ca-bundle.crt" // default on centos
  };
  #if MAC_BUILD
    // If we are building on mac, then add one more path to look for certificate file, i.e.,
    // the current executable path (since we bundle certificate file together with distribution)
    string certpath = getExecutablePathOnMac() + "/resources/ca-certificates.crt";
    standardPathLocations[ARR_SIZE - 1] = certpath.c_str();
  #endif
  if (!certificateFile.empty()) {
    DXLOG(logINFO) << "Option '--certificate-file' present, and value is: '" << certificateFile << "'";
    CA_CERT() = certificateFile;
    return;
  } else {
    if (CA_CERT().empty()) {
      DXLOG(logINFO) << "--certificate-file is not specified, and environment variable DX_CA_CERT is not present." << endl;
      #if WINDOWS_BUILD
        DXLOG(logINFO) << " For Windows version, we don't look for CA certificate in standard location, but rather use the curl default.";
        return;
      #else
        DXLOG(logINFO) << " Will look in standard locations for certificate file (to verify peers)";
        // Look into standard locations
        for (unsigned i = 0; i < ARR_SIZE; ++i) {
          DXLOG(logINFO) << "\tChecking in location: '" << standardPathLocations[i] << "'";
          fs::path p (standardPathLocations[i]);
          if (fs::exists(p)) {
            DXLOG(logINFO) << " ... Found! Will use it.";
            CA_CERT() = standardPathLocations[i];
            return;
          }
          DXLOG(logINFO) << " ... not found.";
        }

        throw runtime_error("Unable to find certificate file (for verifying authenticity of the peer over SSL connection) in any of the standard locations.\n"
                            "Please use the option '--certificate-file' to specify its location, or set it to string 'NOVERIFY' to disable "
                            "authenticity check of the remote host (not recommended).");
      #endif
    } else {
      // use the DX_CA_CERT value (already set by dxcpp's static initializer).
      DXLOG(logINFO) << "'--certificate-file' option is absent, but 'DX_CA_CERT' is present, value is: '" << CA_CERT() << "'. Will use it.";
      return;
    }
  }
}

/*
 * This function does the following:
 *
 * - If the --auth-token and --apiserver-* params are not provided, set
 *   them from values in dxcpp (dx::config::*).
 *
 * - If the --auth-token, --apiserver-* params are provided, set the values
 *   in dxcpp (dx::config::*), so that dxcpp uses correct host, token, etc.
 *
 * - Throw an error if a required parameter is not set anywhere (provided
 *   on command line, or in dx::config).
 */
void Options::setApiserverDxConfig() {
  using namespace dx::config; // for SECURITY_CONTEXT(), APISERVER_*(), etc

  if (authToken.empty()) {
    // If --auth-token flag is not used, check that dx::config::SECURITY_CONTEXT() has a auth token, else throw
    if (SECURITY_CONTEXT().size() == 0)
      throw runtime_error("No Authentication token found, please provide a correct auth token (you may use --auth-token option)");
  } else {
    DXLOG(logINFO) << "Setting dx::config::SECURITY_CONTEXT() from value provided at run time: '" << authToken << "'";
    SECURITY_CONTEXT() = dx::JSON::parse("{\"auth_token_type\": \"Bearer\", \"auth_token\": \"" + authToken + "\"}");
  }

  if (!apiserverProtocol.empty()) {
    DXLOG(logINFO) << "Setting dx::config::APISERVER_PROTOCOL from value provided at run time: '" << apiserverProtocol << "'";
    APISERVER_PROTOCOL() = apiserverProtocol;
  } else {
    apiserverProtocol = APISERVER_PROTOCOL();
    DXLOG(logINFO) << "Using apiserver protocol from dx::config::APISERVER_PROTOCOL: '" << apiserverProtocol << "'";
  }
  if (apiserverPort != -1) {
    DXLOG(logINFO) << "Setting dx::config::APISERVER_PORT from value provided at run time: '" << apiserverPort << "'";
    APISERVER_PORT() = boost::lexical_cast<string>(apiserverPort);
  } else {
    apiserverPort = boost::lexical_cast<int>(APISERVER_PORT());
    DXLOG(logINFO) << "Using apiserver port from dx::config::APISERVER_PORT: '" << apiserverPort << "'";
  }
  if (!apiserverHost.empty()) {
    DXLOG(logINFO) << "Setting dx::config::APISERVER_HOST from value provided at run time: '" << apiserverHost << "'";
    APISERVER_HOST() = apiserverHost;
  } else {
    apiserverHost = APISERVER_HOST();
    DXLOG(logINFO) << "Using apiserver host from dx::config::APISERVER_HOST: '" << apiserverHost << "'";
  }
  // Now check that dxcpp has all of the required apiserver params set
  if (APISERVER().empty()) {
    throw runtime_error("At least one of apiserver host/port/protocol is not specified, unable to continue without this information."
                        "Please use --apiserver-host, --apiserver-port, --apiserver-protocol to provide this info on command line");
  }

  string lowerCaseProt = APISERVER_PROTOCOL();
  std::transform(lowerCaseProt.begin(), lowerCaseProt.end(), lowerCaseProt.begin(), ::tolower); // convert to lower case

  if (lowerCaseProt == "https") {
    setCertificateFile(certificateFile);
  }
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
  cerr << "Usage: " << programName << " [options] <file> [...]" << endl
       << endl
       << (*visible_opts) << endl;
}

void Options::validate() {
  if (!files.empty()) {
    // - Check that all file actually exist
    // - Resolve all symlinks
    // - Ensure that the inputs are regular files (not directories, etc.)
    for (unsigned i = 0; i < files.size(); ++i) {
      fs::path p(files[i]);
      if (!fs::exists(p)) {
        throw runtime_error("File \"" + files[i] + "\" does not exist");
      }
      if (fs::is_symlink(p)) {
        p = fs::read_symlink(p);
        files[i] = p.string();
      }
      if (fs::is_directory(p)) {
        throw runtime_error("Argument " + files[i] + " is a directory; recursive directory upload is not currently supported.");
      } else if (!fs::is_regular_file(p)) {
        throw runtime_error("Argument " + files[i] + " is not a regular file.");
      }
    }
  } else {
    throw runtime_error("Must specify at least one file to upload");
  }

  if (names.size() == 0) {
    // Get each file object name from the local name of the corresponding
    // file.
    for (int i = 0; i < (int) files.size(); ++i) {
      fs::path p(files[i]);
      names.push_back(p.filename().string());
    }
  } else if (names.size() != files.size()) {
    // If names were specified, there must be exactly as many names as
    // files.
    throw runtime_error("Must specify a name for each file; there are " +
                        boost::lexical_cast<string>(files.size()) + " files, but only " +
                        boost::lexical_cast<string>(names.size()) + " names were provided.");
  }

  if (projects.empty()) {
    if (!dx::config::CURRENT_PROJECT().empty()) {
      DXLOG(logINFO) << "No project was explicitly specified, will use from dx::config::CURRENT_PROJECT = '" << dx::config::CURRENT_PROJECT() << "'";
      projects.push_back(dx::config::CURRENT_PROJECT());
    }
    else
      throw runtime_error("A project must be specified (or present in environment variables/config file). You may use --project to specify project id/name on command line");
  }
  // Now if only 1 project is specified, make them equal to number of files.
  if (projects.size() == 1) {
    DXLOG(logINFO) << "Only one project was found (specified explicitly, or retrieved from environment variables). Will use it for all input file(s)." << endl;
    // If one project was specified, use that for all files.
    while (projects.size() < files.size()) {
      projects.push_back(projects[0]);
    }
  } else {
    // If n projects (n > 1) were specified, then "n" should be exactly same as total number of files
    if (projects.size() != files.size()) {
      // If (multiple) projects were specified, there must be exactly as many
      // projects as files.
      throw runtime_error("Must specify a project for each file; there are " +
                          boost::lexical_cast<string>(files.size()) + " files, but only " +
                          boost::lexical_cast<string>(projects.size()) + " projects were provided.");
    }
  }

  if (folders.empty()) {
    throw runtime_error("A folder must be specified");
  } else if (folders.size() == 1) {
    // If one folder was specified, use that for all files.
    while (folders.size() < files.size()) {
      folders.push_back(folders[0]);
    }
  } else if (folders.size() != files.size()) {
    // If (multiple) folders were specified, there must be exactly as many
    // folders as files.
    throw runtime_error("Must specify a folder for each file; there are " +
                        boost::lexical_cast<string>(files.size()) + " files, but only " +
                        boost::lexical_cast<string>(folders.size()) + " folders were provided.");
  }

  /*
   * At this point, we should have the same number of names, folders,
   * projects, and (local) files. If this is not the case, this is an
   * internal error -- something that should "never happen", not something
   * that can be addressed by the user.
   */
  assert(names.size() == files.size());
  assert(folders.size() == files.size());
  assert(projects.size() == files.size());

  if (readThreads < 1) {
    ostringstream msg;
    msg << "Number of read threads must be positive: " << readThreads;
    throw runtime_error(msg.str());
  }
  if (compressThreads < 1) {
    ostringstream msg;
    msg << "Number of compression threads must be positive: " << compressThreads;
    throw runtime_error(msg.str());
  }
  if (uploadThreads < 1) {
    ostringstream msg;
    msg << "Number of upload threads must be positive: " << uploadThreads;
    throw runtime_error(msg.str());
  }
  if (chunkSize < 5 * 1024 * 1024) {
    ostringstream msg;
    msg << "Minimum chunk size is " << (5 * 1024 * 1024) << " (5 MB): " << chunkSize;
    throw runtime_error(msg.str());
  }
  if (tries < 1) {
    ostringstream msg;
    msg << "Number of tries per chunk must be positive: " << tries;
    throw runtime_error(msg.str());
  }

  if (throttle < 0) {
    cerr << "Upload throttling is disabled." << endl;
  } else if (throttle < 4 * 1024) {
    throw runtime_error("Uploads are throttled to " + boost::lexical_cast<string>(throttle) + " bytes/sec, which is less than 4 Kbytes/sec. Choose a larger value.");
  } else if (throttle < 256 * 1024) {
    cerr << "WARNING: Uploads are throttled to " << throttle << " bytes/sec, which is less than 256 KBytes/sec. We recommend allowing higher speeds for better performance." << endl;
  } else {
    cerr << "Uploads are throttled to " << throttle << " bytes/sec." << endl;
  }

  // Check that at most one import flag is present.
  int countImportFlags = 0;
  countImportFlags += (reads) ? 1 : 0;
  countImportFlags += (pairedReads) ? 1 : 0;
  countImportFlags += (mappings) ? 1 : 0;
  countImportFlags += (variants) ? 1 : 0;
  if (countImportFlags > 1) {
    ostringstream msg;
    msg << "Only one of these flags can be used in a single call: --reads, --paired-reads, --mappings, and --variants.";
    throw runtime_error(msg.str());
  }
  if ((mappings || variants) && refGenome.empty()) {
    ostringstream msg;
    msg << "Reference Genome must be specified (using --ref-genome flag) if --mappings, or --variants is present.";
    throw runtime_error(msg.str());
  }
  if (!mappings && !variants && !refGenome.empty()) {
    ostringstream msg;
    msg << "Reference Genome (--ref-genome) can only be specified if --mappings, or --variants is present.";
    throw runtime_error(msg.str());
  }
  if (pairedReads && (files.size() % 2)) {
    ostringstream msg;
    msg << "Even number of files (pairs of left, and right reads) must be provided if --paired-reads flag is present";
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
    out << "  projects:";
    for (unsigned int i = 0; i < opt.projects.size(); ++i)
      out << " \"" << opt.projects[i] << "\"";
    out << endl;

    out << "  folders:";
    for (unsigned int i = 0; i < opt.folders.size(); ++i)
      out << " \"" << opt.folders[i] << "\"";
    out << endl;

    out << "  names:";
    for (unsigned int i = 0; i < opt.names.size(); ++i)
      out << " \"" << opt.names[i] << "\"";
    out << endl;

    out << "  files:";
    for (unsigned int i = 0; i < opt.files.size(); ++i)
      out << " \"" << opt.files[i] << "\"";
    out << endl;

    out << "  read-threads: " << opt.readThreads << endl
        << "  compress-threads: " << opt.compressThreads << endl
        << "  upload-threads: " << opt.uploadThreads << endl
        << "  chunk-size: " << opt.chunkSize << endl
        << "  tries: " << opt.tries << endl
        << "  do-not-compress: " << opt.doNotCompress << endl
        << "  progress: " << opt.progress << endl
        << "  verbose: " << opt.verbose << endl
        << "  wait on close: " << opt.waitOnClose << endl
        << "  do-not-resume: " << opt.doNotResume << endl
        << "  reads: " << opt.reads << endl
        << "  paired-reads: " << opt.pairedReads << endl
        << "  mappings: " << opt.mappings << endl
        << "  variants: " << opt.variants << endl
        << "  ref-genome: " << opt.refGenome << endl
        << "  certificate-file" << opt.certificateFile << endl
        << "  no-round-robin-dns" << opt.noRoundRobinDNS << endl;
  }
  return out;
}
