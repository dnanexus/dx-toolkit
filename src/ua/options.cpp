#include "options.h"

using namespace std;

#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

#include "dxjson/dxjson.h"

string envVarMapper(const string &var) {
  if (var == "DX_APISERVER_HOST") {
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
  visible_opts = new po::options_description("Allowed options");
  visible_opts->add_options()
    ("help,h", "Produce a help message")
    ("version", "Print the version")
    ("auth-token,a", po::value<string>(&authToken), "Specify the authentication token")
    ("project,p", po::value<string>(&project), "Name or ID of the destination project")
    ("folder,f", po::value<string>(&folder)->default_value("/"), "Name of the destination folder")
    ("name,n", po::value<string>(&name), "Name of the file to be created")
    ("compress-threads,c", po::value<int>(&compressThreads)->default_value(2), "Number of parallel compression threads")
    ("upload-threads,u", po::value<int>(&uploadThreads)->default_value(4), "Number of parallel upload threads")
    ("chunk-size,s", po::value<int>(&chunkSize)->default_value(100 * 1000 * 1000), "Size of chunks in which the file should be uploaded")
    ("tries,r", po::value<int>(&tries)->default_value(3), "Number of tries to upload each chunk")
    ("compress,z", po::bool_switch(&compress), "Compress chunks before upload")
    ("progress,g", po::bool_switch(&progress), "Report upload progress")
    ("verbose,v", po::bool_switch(&verbose), "Verbose logging")
    ;

  hidden_opts = new po::options_description();
  hidden_opts->add_options()
    ("file", po::value<string>(&file), "File to upload")
    ;

  command_line_opts = new po::options_description();
  command_line_opts->add(*visible_opts);
  command_line_opts->add(*hidden_opts);

  env_opts = new po::options_description();
  env_opts->add_options()
    ("apiserver-host", po::value<string>(&apiserverHost), "API server host")
    ("apiserver-port", po::value<int>(&apiserverPort), "API server port")
    ("project", po::value<string>(&project), "ID of the destination project")
    ;

  pos_opts = new po::positional_options_description();
  pos_opts->add("file", -1);
}

void Options::parse(int argc, char * argv[]) {
  po::store(po::command_line_parser(argc, argv).options(*command_line_opts).positional(*pos_opts).run(), vm);
  po::notify(vm);

  po::store(parse_environment(*env_opts, envVarMapper), vm);
  po::notify(vm);

  if (name.empty()) {
    fs::path p(file);
    name = p.filename().string();
  }

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

void Options::printHelp() {
  cerr << (*visible_opts) << endl;
}

void Options::validate() {
  if (file.empty()) {
    throw runtime_error("A file to upload must be specified");
  }
  if (authToken.empty()) {
    throw runtime_error("An authentication token must be provided");
  }
  if (apiserverHost.empty()) {
    throw runtime_error("An API server must be specified");
  }
  if (apiserverPort < 1) {
    ostringstream msg;
    msg << "Invalid API server port: " << apiserverPort;
    throw runtime_error(msg.str());
  }
  if (project.empty()) {
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
  if (chunkSize < 1) {
    ostringstream msg;
    msg << "Chunk size must be positive: " << chunkSize;
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
        << "  API server host: " << opt.apiserverHost << endl
        << "  API server port: " << opt.apiserverPort << endl

        << "  project: " << opt.project << endl
        << "  folder: " << opt.folder << endl
        << "  name: " << opt.name << endl
        << "  file: " << opt.file << endl

        << "  compression threads: " << opt.compressThreads << endl
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
