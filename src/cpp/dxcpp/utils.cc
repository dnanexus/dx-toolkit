#ifndef WINDOWS_BUILD
  #include <pwd.h>
  #include <unistd.h>
#endif
#include <cstdlib>
#include <openssl/md5.h>
#include <sstream>
#include <string>
#include <iomanip>
#include "utils.h"

using namespace std;

// Return path to user's home directory
string getUserHomeDirectory() {
#ifndef WINDOWS_BUILD
  // see if HOME env variable is set
  if (getenv("HOME") != NULL)
    return getenv("HOME");
  
  // else get from password database (if POSIX system)
  struct passwd *pw = getpwuid(getuid());
  return pw->pw_dir;
#else
  // For windows system .. the user home directory path is
  // concatenation of two env variables: %SYSTEMDIR% %HOMEPATH% variable
  // http://en.wikipedia.org/wiki/Environment_variable#Default_Values_on_Microsoft_Windows
  string sdir = "C:", hpath;
  if (getenv("SYSTEMDIR") != NULL)
    sdir = getenv("SYSTEMDIR");
  if (getenv("HOMEPATH") != NULL)
    hpath = getenv("HOMEPATH");
  return sdir + hpath;
  // TODO: Should we get home directory in windows using function SHGetFolderPath() in windows.h ?
#endif
}

string joinPath(const string &first_path, const string &second_path, const string &third_path) {
#ifndef WINDOWS_BUILD
  string result = first_path + "/" + second_path;
  if (third_path != "") {
    result = result + "/" + third_path;
  }
#else
  string result = first_path + "\\" + second_path;
  if (third_path != "") {
    result = result + "\\" + third_path;
  }
#endif
  return result;
}

std::string getHexifiedMD5(const unsigned char *ptr, const unsigned long size) {
  unsigned char md5[MD5_DIGEST_LENGTH];
  MD5(ptr, size, md5);
  
  // convert to hex string & return
  std::ostringstream oss; 
  oss << std::setfill('0');    
  for (unsigned i = 0; i < sizeof(md5)/sizeof(md5[0]); ++i) {   
    oss << std::setw(2) << std::hex << static_cast<int>(md5[i]);
  }
  return oss.str();
}

std::string getHexifiedMD5(const vector<char> &inp) {
  if (inp.size() == 0) {
    return getHexifiedMD5(reinterpret_cast<const unsigned char*>(""), 0);
  } else {
    return getHexifiedMD5(reinterpret_cast<const unsigned char*>(&inp[0]), inp.size());
  }
  // not reachable
}

std::string getHexifiedMD5(const std::string &inp) {
  return getHexifiedMD5(reinterpret_cast<const unsigned char*>(inp.data()), inp.size());
}

