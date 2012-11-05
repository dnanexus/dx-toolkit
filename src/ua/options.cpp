#include "options.h"

using namespace std;

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <boost/filesystem/operations.hpp>
#include "log.h"
#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

namespace fs = boost::filesystem;

Options::Options() {
  int defaultCompressThreads = int(boost::thread::hardware_concurrency()) - 1;
  if (defaultCompressThreads < 1) {
    defaultCompressThreads = 1;
  }

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
    ("name,n", po::value<vector<string> >(&names), "Name of the file to be created")
    ("read-threads", po::value<int>(&readThreads)->default_value(1), "Number of parallel disk read threads")
    ("compress-threads,c", po::value<int>(&compressThreads)->default_value(defaultCompressThreads), "Number of parallel compression threads")
    ("upload-threads,u", po::value<int>(&uploadThreads)->default_value(4), "Number of parallel upload threads")
    ("chunk-size,s", po::value<int>(&chunkSize)->default_value(100 * 1000 * 1000), "Size (in bytes) of chunks in which the file should be uploaded")
    ("tries,r", po::value<int>(&tries)->default_value(3), "Number of tries to upload each chunk")
    ("do-not-compress", po::bool_switch(&doNotCompress), "Do not compress file(s) before upload")
    ("progress,g", po::bool_switch(&progress), "Report upload progress")
    ("verbose,v", po::bool_switch(&verbose), "Verbose logging")
    ("wait-on-close", po::bool_switch(&waitOnClose), "Wait for file objects to be closed before exiting")
    ("do-not-resume", po::bool_switch(&doNotResume), "Do not attempt to resume any incomplete uploads")
    // Options for running import apps
    ("reads", po::bool_switch(&reads), "After uploading is complete, run import app to convert file(s) to Reads object(s)")
    ("paired-reads", po::bool_switch(&pairedReads), "Same as --reads option, but assumes file sequence to be pairs of left, and right reads (e.g., L1 R1 L2 R2 L3 R3 ...)")
    ("mappings", po::bool_switch(&mappings), "After uploading is complete, run import app to convert file(s) to Mappings object(s)")
    ("variants", po::bool_switch(&variants), "After uploading is complete, run import app to convert file(s) to Variants object(s)")
    ("ref-genome", po::value<string>(&refGenome), "ID or name of the reference genome (must be present if and only if --mappings, or, --variants flag is used)")
    ;

  hidden_opts = new po::options_description();
  hidden_opts->add_options()
    ("file", po::value<vector<string> >(&files), "File to upload")
    ("apiserver-protocol", po::value<string>(&apiserverProtocol), "API server protocol")
    ("apiserver-host", po::value<string>(&apiserverHost), "API server host")
    ("apiserver-port", po::value<int>(&apiserverPort)->default_value(-1), "API server port") 
    ;

  command_line_opts = new po::options_description();
  command_line_opts->add(*visible_opts);
  command_line_opts->add(*hidden_opts);
  
  pos_opts = new po::positional_options_description();
  pos_opts->add("file", -1);
}

void Options::parse(int argc, char * argv[]) {
  po::store(po::command_line_parser(argc, argv).options(*command_line_opts).positional(*pos_opts).run(), vm);
  po::notify(vm);
  Log::enabled = verbose;
  if (authToken.empty()) {
    char * dxSecurityContext = getenv("DX_SECURITY_CONTEXT");
    if (dxSecurityContext != NULL) {
      dx::JSON secContext = dx::JSON::parse(dxSecurityContext);
      if (secContext.has("auth_token")) {
        authToken = secContext["auth_token"].get<string>();
      }
    }
  }

  /*
   * Incorporate values read by loadFromEnvironment in dxcpp. This handles
   * the contents of enviornment variables, and ~/.dnanexus_config/environment.
   */
  if (apiserverProtocol.empty()) {
    LOG << "Setting apiServerProtocol from g_APISERVER_PROTOCOL: " << g_APISERVER_PROTOCOL << endl;
    apiserverProtocol = g_APISERVER_PROTOCOL;
  }
  if (apiserverHost.empty()) {
    LOG << "Setting apiServerHost from g_APISERVER_HOST: " << g_APISERVER_HOST << endl;
    apiserverHost = g_APISERVER_HOST;
  }
  if (apiserverPort == -1) {
    LOG << "Setting apiServerPort from g_APISERVER_PORT: " << g_APISERVER_PORT << endl;
    apiserverPort = boost::lexical_cast<int>(g_APISERVER_PORT);
  }
  if (authToken.empty()) {
    if (g_SECURITY_CONTEXT_SET) {
      if (g_SECURITY_CONTEXT.has("auth_token")) {
        LOG << "Setting authToken from g_SECURITY_CONTEXT: " << g_SECURITY_CONTEXT["auth_token"].get<string>() << endl;
        authToken = g_SECURITY_CONTEXT["auth_token"].get<string>();
      }
    }
  }
  if (projects.empty()) {
    if (!g_PROJECT_CONTEXT_ID.empty()) {
      LOG << "Adding to projects from g_PROJECT_CONTEXT_ID: " << g_PROJECT_CONTEXT_ID << endl;
      projects.push_back(g_PROJECT_CONTEXT_ID);
    }
  }
  if (projects.empty()) {
    if (!g_WORKSPACE_ID.empty()) {
      LOG << "Adding to projects from g_WORKSPACE_ID: " << g_WORKSPACE_ID << endl;
      projects.push_back(g_WORKSPACE_ID);
    }
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
    for (unsigned i = 0; i < files.size(); ++i) {
      fs::path p (files[i]);
      if (!fs::exists(p)) {
        throw runtime_error("File \"" + files[i] + "\" does not exist");
      }
      if (fs::is_symlink(p)) {
        files[i] = fs::read_symlink(p).string(); 
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
    throw runtime_error("A project must be specified");
  } else if (projects.size() == 1) {
    // If one project was specified, use that for all files.
    while (projects.size() < files.size()) {
      projects.push_back(projects[0]);
    }
  } else if (projects.size() != files.size()) {
    // If (multiple) projects were specified, there must be exactly as many
    // projects as files.
    throw runtime_error("Must specify a project for each file; there are " +
                        boost::lexical_cast<string>(files.size()) + " files, but only " +
                        boost::lexical_cast<string>(projects.size()) + " projects were provided.");
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
   * At this point, we should have the same name of names, folders,
   * projects, and (local) files. If this is not the case, this is an
   * internal error -- something that should "never happen", not something
   * that can be addressed by the user.
   */
  assert(names.size() == files.size());
  assert(folders.size() == files.size());
  assert(projects.size() == files.size());

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

    out << "  read threads: " << opt.readThreads << endl
        << "  compress threads: " << opt.compressThreads << endl
        << "  upload threads: " << opt.uploadThreads << endl
        << "  chunkSize: " << opt.chunkSize << endl
        << "  tries: " << opt.tries << endl
        << "  do-not-compress: " << opt.doNotCompress << endl
        << "  progress: " << opt.progress << endl
        << "  verbose: " << opt.verbose << endl
        << "  wait on close: " << opt.waitOnClose << endl
        << "  do-not-resume: " << opt.doNotResume << endl
      ;
  }
  return out;
}
