// Copyright (C) 2013-2016 DNAnexus, Inc.
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

#include "mime.h"

#include <cstdio>
#include <iostream>
#include <exception>
#include <stdexcept>
#include <algorithm> 
#include <functional> 
#include <cctype>
#include <locale>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <magic.h>

#include "dxcpp/dxlog.h"

using namespace std;
using namespace dx;

// Note currently POSIX_BUILD is same as !WINDOWS_BUILD
#define POSIX_BUILD (MAC_BUILD || LINUX_BUILD)

#if WINDOWS_BUILD
  #include <windows.h>
  // This additional code is required for Windows build, since Magic database is not present,
  // we simply bundle the .mgc file
  string MAGIC_DATABASE_PATH;	
#endif

static std::string INDENT = "  - "; // all log messages in this file will start with this string

// Returns a new string with whitespaces removed from start & end
// Note: Instead of using Boost regex (which is *NOT* a header only library)
//       we remove whitespaces ourselves
string trim(const string &str) {
  // Inspired from: http://stackoverflow.com/a/217605
  
  string out = str;
  // trim from start
  out.erase(out.begin(), std::find_if(out.begin(), out.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
  // trim from end
  out.erase(std::find_if(out.rbegin(), out.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), out.end());
  
  return out;
}

// This function is intended to solve a bug which only surface on some archaic platforms
// (and, when compiled with very old libmagic version, e.g., we faced this on CentOS 5.4)
// The problem is this:
//   1) "file" command on these systems do not understand "--mime-type" flag (v4.17 was available where we tested)
//   2) libmagic returns this string as mime type: "application/x-executable, for GNU/Linux 2.6.9, statically linked, for GNU/Linux 2.6.9, not stripped"
// 
// APIServer however refuse to accept any character outside [33, 126] range for "media" type (note the spaces in the libmagic output).
// We will be more strict, and create a whitelist of characters that should appear in mime type,
// and only use the string until first such enounter, or the end of the string.
// 
// Note: Call this function as the last transformation over the output string (e.g., after trim())
string sanitizeMediaType(const string &str) {
  string out;
  string allowedExtraChars = "/-.><+"; // these characters, combined with isalnum() chars, should handle almost all cases we care about.
  DXLOG(logINFO) << "Sanitizing media type string ('" << str << "')";
  for (unsigned i = 0; i < str.length(); ++i) {
    if (isalnum(str[i]) || allowedExtraChars.find(str[i]) != string::npos)
      out += str[i];
    else {
      DXLOG(logWARNING) << "An invalid character('" << str[i] <<"') found in the mime type string ('"
                        << str << "'). Will only use string upto first such occurrence ('" << out << "')";
      return out;
    }
  }
  DXLOG(logINFO) << "Nothing to sanitize in the media type string";
  return out;
}

// - This function is our very last resort to detect compressed file (using file extension!)
// - I added some of the common compressed file extensions to this list
// - If the extension is one of the known compressed type, we return appropriate mime type
//   else we return back empty string
string detectCompressTypesUsingExtension(const string &filePath) {
  // http://www.boost.org/doc/libs/1_53_0/libs/filesystem/doc/reference.html#path-extension
  string ext = boost::filesystem::path(filePath).extension().string(); // returns something like ".txt"
  DXLOG(logINFO) << "File extension is '" << ext << "', will try and match up against some common extensions ..."; 
  if (ext == "") // no extension present in the file, can't do anything
    return "";
  
  if (ext == ".bz")
    return "application/x-bzip";
  
  if (ext == ".bz2" || ext == ".boz")
    return "application/x-bzip2";
  
  if (ext == ".zip")
    return "application/zip";
  
  if (ext == ".gz")
    return "application/x-gzip";

  if (ext == ".7z")
    return "application/x-7z-compressed";
  
  if (ext == ".lzh" || ext == ".lha")
    return "application/x-lzh-compressed";
  
  if (ext == ".xz")
    return "application/x-xz";

  if (ext == ".rar")
    return "application/x-rar-compressed";
 
  if (ext == ".gtar")
    return "application/x-gtar";
  
  // If the extension doesn't match any of these form
  // return empty string
  return "";
}

#if WINDOWS_BUILD
// We package the magic file with UA for windows
// This function returns the path to that magic file (by finding out current executable path)
void setMagicDBPath() {
  if (MAGIC_DATABASE_PATH.size() > 0)
    return;

  // Find out the current process's directory
  char buffer[32768] = {0}; // Maximum path length in windows (approx): http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx#maxpath
  if (!GetModuleFileName(NULL, buffer, 32767)) {
    throw runtime_error("Unable to get current process's directory using GetModuleFileName() .. GetLastError() = " + boost::lexical_cast<string>(GetLastError()) + "\n");
  }
  string processPath = buffer;
  size_t found = processPath.find_last_of("\\");
  found = (found != string::npos) ? found : 0;
  MAGIC_DATABASE_PATH = processPath.substr(0, found) + "\\magic";
}
#endif

/* 
 * - Returns the MIME type for a file (of this format: "type/subType")
 * - We do not try to uncompress an archive, rather return the mime type for compressed file.
 * - Throw runtime_error if the file path (fpath) is invalid, or if some other
 *   internal error occurs.
 */
// Note: This function is NOT thread-safe (since it redirects "stderr" to /dev/null temporarily)
//       At most one instance of this function should run at any time (else "stderr" might point to /dev/null forever)
string getMimeTypeUsingLibmagic(const string& filePath) {
  string magic_output;
  magic_t magic_cookie;
  magic_cookie = magic_open(MAGIC_MIME | MAGIC_NO_CHECK_COMPRESS | MAGIC_SYMLINK | MAGIC_ERROR);
  if (magic_cookie == NULL) {
    throw runtime_error("error allocating magic cookie (libmagic)");
  }

#if !WINDOWS_BUILD
	const char *ptr_to_db = NULL; // NULL means look in default location
#else
	setMagicDBPath();
	const char *ptr_to_db = MAGIC_DATABASE_PATH.c_str();
#endif

#if POSIX_BUILD
  // We redirect stderr momentarily, because "libmagic" prints bunch of warning (which we don't care about much)
  // on stderr, and the easiest way to get rid of them is to redirect stderr to /dev/null (see PTFM-4636)
  FILE *stderr_backup = stderr; // store original stderr FILE pointer
  FILE *devnull = fopen("/dev/null", "w");
  if (devnull == NULL) {
    throw runtime_error("Unable to open either: '/dev/null': Unexpected");
  }
  stderr = devnull; // redirect stderr to /dev/null, so that warning by magic_load() are not printed.
#endif
  int errorCode = magic_load(magic_cookie, ptr_to_db);
#if POSIX_BUILD
  stderr = stderr_backup; // restore original value of stderr
  fclose(devnull);
#endif

  if (errorCode) {
    string errMsg = magic_error(magic_cookie);
    magic_close(magic_cookie);
#if !WINDOWS_BUILD
    throw runtime_error("cannot load magic database - '" + errMsg + "'");
#else
    throw runtime_error("cannot load magic database - '" + errMsg + "'" + " Magic DB path = '" + MAGIC_DATABASE_PATH + "'");
#endif
  } 
  magic_output = magic_file(magic_cookie, filePath.c_str());
  magic_close(magic_cookie);

  // magic_output will be of this format: "type/subType; charset=.."
  // we just want to return "type/subType"
  return magic_output.substr(0, magic_output.find(';'));
}

#if POSIX_BUILD
  // executes a command using popen
  // @param cmd The actual command to be executed
  // @param output This will contain the stdout output of the command
  // @return a boolean indicating failure/success of command execution
  bool execCommand(const string &cmd, string &output) {
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe)
      return false;
    
    bool success = true;
    
    // Note: appending to std::string can throw, however we still want to close the pipe 
    //       in those circumstances, hence the try-catch block
    try {
      char buffer[1024];
      while(!feof(pipe)) {
        if(fgets(buffer, 1024, pipe) != NULL)
          output += buffer;
      }
    } catch(const std::length_error &e1) {
      DXLOG(logINFO) << "Exception thrown while appending to string in execCommand(), error = " << e1.what();
      success = false;
    } catch(const std::bad_alloc &e2) {
      DXLOG(logINFO) << "Exception thrown while appending to string in execCommand(), error = " << e2.what();
      success = false;
    }
    int ret = pclose(pipe);
    if (ret == -1) {
      // Call to plcose() failed -> This should almost never happen.
      // log this event
      DXLOG(logINFO) << "Call to pclose() failed errno =  " << errno;
      // TODO: What else to do in this case ?
    }
    if (ret > 0) {
      DXLOG(logINFO) << "The command: '" << cmd << "'<< returned with non-zero exit code (" << ret << "), stdout = '" << output << "' ... ";
    }
    success = success && (ret == 0);
    return success;
  }
  // This function returns mime type of the given local file
  // (internally executes the "file" command)
  // If "file" command execution fails for some reason, we try and get mime type from
  // libmagic, and if that fails as well we match extension of the file to a few known compressed
  // types.
  // Note: - We assume that existence of file has beem already checked
  //       - Since we use "file" command, this function only make sense for
  //         POSIX platforms
  string getMimeTypeForPosixSystems(const string &filePath) {
    // We first create a symlink for the file (so that we don't have to deal with
    // the escaping of file name (so that bash won't interpret them)
    bool fs_success = true; // will be false is any of the boost filesystem functions fail
    namespace fs = boost::filesystem;
    fs::path sp; // path of temp symlink file we will create
    {
      try {
        sp = fs::unique_path(fs::temp_directory_path().string() + "/ua-symlink-%%%%%%%%%%%%%.tmp"); // Create it in a temp directory
        DXLOG(logINFO) << "Generated path for unique temp file: '" << sp.string() << "'";
        
        // We need to find full system path of the file, 
        // because otherwise symlink we create will be absurd
        // (because we create symlink in a differnt directory than the one user used to specify the file path)
        fs::path complete_path = fs::system_complete(filePath);
        fs::create_symlink(complete_path, sp);
        DXLOG(logINFO) << "Created symlink ('" << sp.string() << "') to file '" << complete_path.string() << "'";
      } catch (exception &boost_err) {
        DXLOG(logINFO) << "An exception occured while trying to create a temp symlink to existing file. Error message = '" << boost_err.what() << "'";
        fs_success = false;
      }
    }
    if (fs_success) {
      string cmd = "file -L --brief --mime-type " + sp.string() + " 2>&1";
      string sout;
      bool exec_success = execCommand(cmd, sout); // actually execute the file command

      // On older versions of 'file', --mime-type option is not recognized, try the "-i/-mime" flag instead (which is more verbose)
      // sanitizeMediaType() should take care of the verboseness of "-i" flag
      if (!exec_success) {
        sout.clear();
        DXLOG(logINFO) << "Previous call to 'file' command (with --mime-type) failed, trying with -i instead (for older versions of 'file')";
        cmd = "file -L --brief -i " + sp.string() + " 2>&1";
        exec_success = execCommand(cmd, sout); // execute the command
      }

      DXLOG(logINFO) << "Removing the temp symlink file ('" << sp.string() << "')";
      fs::remove(sp); // remove the temp symlink we created
      if (exec_success) {
        return sanitizeMediaType(trim(sout)); // we succesfuly determined mime type using "file" command, return it.
      }
    }
    // We are here => "file" command failed to execute for some reason (or one of the boost filesystem functions failed),
    // we can't really do much at this point, as libmagic *most* likely won't find
    // magic database either, but what the heck! let's try libmagic anyway.
    // As mentioned before, most likely libmagic call will fail too (cannot find magic.db): do catch them!
    try {
      DXLOG(logINFO) << "Unable to get mime type by running 'file' command ... will try to fetch mime type from libmagic ....";
      string temp = getMimeTypeUsingLibmagic(filePath);
      return sanitizeMediaType(temp);
    } catch (runtime_error &e) {
      DXLOG(logINFO) << "Fetching of mime type form libmagic also failed, error = " << e.what();
      // Ignore the error (it was expected anyway!)
    }
    
    // We shall try one last resort --> try to get mime type from file extension!!
    // (we only check for common compressed types), and return empty string if
    // file extension doesn't match few known types.
    DXLOG(logINFO) << "Both, execution of 'file' command, and fetching mime type from libmagic failed ... will try to match extension to common compressed types as a last resort ...";
    return detectCompressTypesUsingExtension(filePath); // no need to call sanitizeMediaType(), since we hand-curate this list anyhow
  }
#endif

/**
 * Given path of the file, this function returns a mime type for it 
 * (or empty string if mime type cannot be detected)
 *
 * @note This function uses different techniques depending on type of platform
 *       (POSIX or non-POSIX). All subtle differences among platforms are taken 
 *       care of by this function.
 *
 * @param filePath path of the file
 * @return mime type of the file (or empty string if no mime type can be detected),
 *         is of the form: "type/subType" (or empty string).
 * @throws runtime_error If file does not exist
 */ 
string getMimeType(const string &filePath) {
  // It's necessary to check file's existence
  // because if an invalid path is given,
  // then bad things will happend, e.g., libmagic silently Seg faults.
  if (!boost::filesystem::exists(boost::filesystem::path(filePath)))
    throw runtime_error("Local file '" + filePath + "' does not exist");
 
  #if POSIX_BUILD
    return getMimeTypeForPosixSystems(filePath);
  #else
    return getMimeTypeUsingLibmagic(filePath);
  #endif
}

/* 
 * Returns true iff the file is detected as 
 * one of the compressed types.
 */
bool isCompressed(const std::string& mimeType) {
  if (mimeType.empty())
    return false; // If no mimeType is given, assume not compressed file

  // This list is mostly from: http://en.wikipedia.org/wiki/List_of_archive_formats,
  // some of the items are added by trying libmagic with few common file formats,
  // and some are from the list here http://svn.apache.org/viewvc/httpd/httpd/trunk/docs/conf/mime.types?view=markup
  //
  // Note: application/x-empty and inode/x-empty are added to treat
  //       the special case of empty file (i.e., not compress them).
  const char* compressed_mime_types[] = {
    "application/x-bzip2",
    "application/zip",
    "application/x-gzip",
    "application/x-lzip",
    "application/x-lzma",
    "application/x-lzop",
    "application/x-xz",
    "application/x-compress",
    "application/x-7z-compressed",
    "application/x-ace-compressed",
    "application/x-alz-compressed",
    "application/x-astrotite-afa",
    "application/x-arj",
    "application/x-cfs-compressed",
    "application/x-lzx",
    "application/x-lzh",
    "application/x-lzh-compressed",
    "application/x-gca-compressed",
    "application/x-apple-diskimage",
    "application/x-dgc-compressed",
    "application/x-dar",
    "application/vnd.ms-cab-compressed",
    "application/x-rar-compressed",
    "application/x-stuffit",
    "application/x-stuffitx",
    "application/x-gtar",
    "application/x-zoo",
    "application/vnd.ms-cab-compressed",
    "application/x-empty",
    "inode/x-empty"
  };
  unsigned numElems = sizeof compressed_mime_types/sizeof(compressed_mime_types[0]);
  for (unsigned i = 0; i < numElems; ++i) {
    if (mimeType == string(compressed_mime_types[i]))
      return true;
  }
  return false;
}
