#include "options.h"

using namespace std;

#include <boost/lexical_cast.hpp>

#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

#include "dxjson/dxjson.h"

void Options::initFromEnvironment() {
  char * dxProjectContextID;
  char * dxSecurityContext;
  char * dxApiserverHost;
  char * dxApiserverPort;

  dxProjectContextID = getenv("DX_PROJECT_CONTEXT_ID");
  if (dxProjectContextID != NULL) {
    project = dxProjectContextID;
  }

  dxSecurityContext = getenv("DX_SECURITY_CONTEXT");
  if (dxSecurityContext != NULL) {
    // parse as JSON and extract auth token
    dx::JSON secContext = dx::JSON::parse(dxSecurityContext);
    authToken = secContext["auth_token"].get<string>();
  }

  dxApiserverHost = getenv("DX_APISERVER_HOST");
  if (dxApiserverHost != NULL) {
    apiserverHost = dxApiserverHost;
  }

  dxApiserverPort = getenv("DX_APISERVER_PORT");
  if (dxApiserverPort != NULL) {
    apiserverPort = boost::lexical_cast<int>(dxApiserverPort);
  }
}

Options::Options(int argc, char * argv[]) {
  desc = new po::options_description("Allowed options");
  desc->add_options()
    ("help,h", "Produce a help message")
    ("auth-token,a", po::value<string>(&authToken), "Specify the authentication token")
    ("project,p", po::value<string>(&project), "Name or ID of the destination project")
    ("folder,f", po::value<string>(&folder)->default_value("/"), "Name of the destination folder")
    ("name,n", po::value<string>(&name), "Name of the file to be created")
    ("threads,t", po::value<int>(&threads)->default_value(4), "Number of parallel upload threads")
    // ("chunks,c", po::value<int>(&chunks)->default_value(-1), "Number of chunks in which the file should be uploaded")
    ("chunk-size,s", po::value<int>(&chunkSize)->default_value(100 * 1000 * 1000), "Size of chunks in which the file should be uploaded")
    ("tries,r", po::value<int>(&tries)->default_value(3), "Number of tries to upload each chunk")
    ("progress,g", po::value<bool>(&progress)->default_value(false), "Report upload progress")
    ("file", po::value<string>(&file), "File to upload")
    ;

  po::positional_options_description pos_desc;
  pos_desc.add("file", -1);

  initFromEnvironment();

  po::store(po::command_line_parser(argc, argv).options(*desc).positional(pos_desc).run(), vm);
  po::notify(vm);

  if (name.empty()) {
    fs::path p(file);
    name = p.filename().string();
  }
}

bool Options::help() {
  return vm.count("help");
}

void Options::printHelp() {
  cerr << (*desc) << endl;
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
  if (threads < 1) {
    ostringstream msg;
    msg << "Number of threads must be positive: " << threads;
    throw runtime_error(msg.str());
  }
  // if (chunks < 1) {
  //   ostringstream msg;
  //   msg << "Number of chunks must be positive: " << chunks;
  //   throw runtime_error(msg.str());
  // }
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

string Options::getFile() {
  return file;
}

ostream &operator<<(ostream &out, const Options &opt) {
  if (opt.vm.count("help")) {
    out << (*(opt.desc)) << endl;
  } else {
    out << "Options:" << endl
        << "  auth token: " << opt.authToken << endl
        << "  API server host: " << opt.apiserverHost << endl
        << "  API server port: " << opt.apiserverPort << endl

        << "  project: " << opt.project << endl
        << "  folder: " << opt.folder << endl
        << "  name: " << opt.name << endl
        << "  file: " << opt.file << endl

        << "  threads: " << opt.threads << endl
        // << "  chunks: " << opt.chunks << endl
        << "  chunkSize: " << opt.chunkSize << endl
        << "  tries: " << opt.tries << endl
        << "  progress: " << opt.progress << endl
      ;
  }
  return out;
}
