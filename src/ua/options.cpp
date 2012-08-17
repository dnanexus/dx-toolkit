#include "options.h"

using namespace std;

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>

namespace fs = boost::filesystem;

#include "dxjson/dxjson.h"

string envVarMapper(const string &var) {
  if (var == "DX_APISERVER_PROTOCOL") {
    return "apiserver-protocol";
  } else if (var == "DX_APISERVER_HOST") {
    return "apiserver-host";
  } else if (var == "DX_APISERVER_PORT") {
    return "apiserver-port";
  } else if (var == "DX_PROJECT_CONTEXT_ID") {
    return "project";
  } else {
    return "";
  }
}

Options::Options() {
  int defaultCompressThreads = int(boost::thread::hardware_concurrency()) - 1;
  if (defaultCompressThreads < 1) {
    defaultCompressThreads = 1;
  }

  vector<string> defaultFolders;
  defaultFolders.push_back("/");

  visible_opts = new po::options_description("Allowed options");
  visible_opts->add_options()
    ("help,h", "Produce a help message")
    ("version", "Print the version")
    ("auth-token,a", po::value<string>(&authToken), "Specify the authentication token")
    ("project,p", po::value<vector<string> >(&projects), "Name or ID of the destination project")
    ("folder,f", po::value<vector<string> >(&folders)->default_value(defaultFolders, "/"), "Name of the destination folder")
    ("name,n", po::value<vector<string> >(&names), "Name of the file to be created")
    ("compress-threads,c", po::value<int>(&compressThreads)->default_value(defaultCompressThreads), "Number of parallel compression threads")
    ("upload-threads,u", po::value<int>(&uploadThreads)->default_value(4), "Number of parallel upload threads")
    ("chunk-size,s", po::value<int>(&chunkSize)->default_value(100 * 1000 * 1000), "Size of chunks in which the file should be uploaded")
    ("tries,r", po::value<int>(&tries)->default_value(3), "Number of tries to upload each chunk")
    ("compress,z", po::bool_switch(&compress), "Compress chunks before upload")
    ("progress,g", po::bool_switch(&progress), "Report upload progress")
    ("verbose,v", po::bool_switch(&verbose), "Verbose logging")
    ;

  hidden_opts = new po::options_description();
  hidden_opts->add_options()
    ("file", po::value<vector<string> >(&files), "File to upload")
    ;

  command_line_opts = new po::options_description();
  command_line_opts->add(*visible_opts);
  command_line_opts->add(*hidden_opts);

  env_opts = new po::options_description();
  env_opts->add_options()
    ("apiserver-protocol", po::value<string>(&apiserverProtocol)->default_value("https"), "API server protocol")
    ("apiserver-host", po::value<string>(&apiserverHost)->default_value("emtest.dnanexus.com"), "API server host")
    ("apiserver-port", po::value<int>(&apiserverPort)->default_value(443), "API server port")
    ("project", po::value<vector<string> >(&projects), "ID of the destination project")
    ;

  pos_opts = new po::positional_options_description();
  pos_opts->add("file", -1);
}

void Options::parse(int argc, char * argv[]) {
  po::store(po::command_line_parser(argc, argv).options(*command_line_opts).positional(*pos_opts).run(), vm);
  po::notify(vm);

  po::store(parse_environment(*env_opts, envVarMapper), vm);
  po::notify(vm);

  if (authToken.empty()) {
    char * dxSecurityContext = getenv("DX_SECURITY_CONTEXT");
    if (dxSecurityContext != NULL) {
      dx::JSON secContext = dx::JSON::parse(dxSecurityContext);
      if (secContext.has("auth_token")) {
        authToken = secContext["auth_token"].get<string>();
      }
    }
  }
}

bool Options::help() {
  return vm.count("help");
}

bool Options::version() {
  return vm.count("version");
}

void Options::printHelp(char * programName) {
  cerr << "Usage: " << programName << " [options] <file>" << endl
       << endl
       << (*visible_opts) << endl;
}

void Options::validate() {
  if (files.empty()) {
    throw runtime_error("Must specify at least one file to upload");
  }
  if (names.empty()) {
    fs::path p(files[0]);
    names.push_back(p.filename().string());
  }
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
  if (projects.empty()) {
    throw runtime_error("A project must be specified");
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
      out << " " << opt.projects[i];
    out << endl;

    out << "  folders:";
    for (unsigned int i = 0; i < opt.folders.size(); ++i)
      out << " " << opt.folders[i];
    out << endl;

    out << "  names:";
    for (unsigned int i = 0; i < opt.names.size(); ++i)
      out << " " << opt.names[i];
    out << endl;

    out << "  files:";
    for (unsigned int i = 0; i < opt.files.size(); ++i)
      out << " " << opt.files[i];
    out << endl;

    out << "  compression threads: " << opt.compressThreads << endl
        << "  upload threads: " << opt.uploadThreads << endl
        << "  chunkSize: " << opt.chunkSize << endl
        << "  tries: " << opt.tries << endl
        << "  compress: " << opt.compress << endl
        << "  progress: " << opt.progress << endl
        << "  verbose: " << opt.verbose << endl
      ;
  }
  return out;
}
