#include <vector>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>
#include "round_robin_dns.h"
#include "dxcpp/dxlog.h"

using namespace std;
// The function getRandomIP() below (both version: Windows & non-windows) memorizes
// their first successful call, and cache ipList are cached after that, and all
// subsequent calls just return a random value from that list.
// (provided host value is same)
static vector<string> ipList;
static string last_host_name;
boost::mutex getRandomIPMutex;

#if !WINDOWS_BUILD
#include <ares.h>
#include <arpa/inet.h>
#include <netdb.h>
  #ifndef CARES_HAVE_ARES_LIBRARY_INIT
    #error "Unexpected: For non-windows platforms, we use c-ares (version > 1.70) library. CARES_HAVE_ARES_LIBRARY_INIT should be present"
  #endif

  // Inspired from https://gist.github.com/mopemope/992777
  
  static void state_cb(void *data, int s, int read, int write) {
    //currently null
  }
   
  static void callback(void *arg, int status, int timeouts, struct hostent *host) {
    if(!host || status != ARES_SUCCESS){
      DXLOG(dx::logWARNING) << "Failed to lookup " << ares_strerror(status);
      return;
    }
    DXLOG(dx::logINFO) << "Found address(es) for host (real name): '" << host->h_name << "'";
    char ip[INET6_ADDRSTRLEN];
   
    for (int i = 0; host->h_addr_list[i]; ++i) {
      inet_ntop(host->h_addrtype, host->h_addr_list[i], ip, sizeof(ip));
      DXLOG(dx::logINFO) << "\t" << i+1 << ". Pushing address '" << ip << "' to ipList";
      ipList.push_back(ip); 
    }
  }
   
  static void wait_ares(ares_channel channel) {
    for(;;){
      struct timeval *tvp, tv;
      fd_set read_fds, write_fds;
      int nfds;
   
      FD_ZERO(&read_fds);
      FD_ZERO(&write_fds);
      nfds = ares_fds(channel, &read_fds, &write_fds);
      if(nfds == 0){
          break;
      }
      tvp = ares_timeout(channel, NULL, &tv);

      select(nfds, &read_fds, &write_fds, NULL, tvp);
      ares_process(channel, &read_fds, &write_fds);
    }
  }
  
  // Use c-ares's ares_gethostbyname() to generate random ip for the host.
  // If the ip addresses could not be resolved, return empty string
  string getRandomIP(const string &host_name) {
    static bool called = false;
    
    // Note: Mutex is to ensure that only one call of this function makes the actual c-ares request
    boost::mutex::scoped_lock ipLock(getRandomIPMutex);
    if (called && last_host_name == host_name) // short circuit the function, if it has been called before
      return (ipList.empty()) ? "" : ipList[rand() % ipList.size()];

    called = true;
    last_host_name = host_name;
    ipList.clear();
    if (host_name.empty()){
      DXLOG(dx::logWARNING) << "getRandomIP() called with empty host ... will simply return empty string";
      return "";
    }
    ares_channel channel;
    struct ares_options options;
    int optmask = 0;
    
    // Note: We do not initialize c-ares library, since previous calls to libcurl must have done that already
    //  int status = ares_library_init(ARES_LIB_INIT_ALL);
    //  if (status != ARES_SUCCESS){
    //    return 1;
    //  }
    options.sock_state_cb = state_cb;
    optmask |= ARES_OPT_SOCK_STATE_CB;
   
    int status = ares_init_options(&channel, &options, optmask);
    if(status != ARES_SUCCESS) {
      DXLOG(dx::logWARNING) << "Unable to initialize ares_channel, status = " << status;
      return "";
    }
   
    ares_gethostbyname(channel, host_name.c_str(), AF_INET, callback, NULL);
    wait_ares(channel);
    ares_destroy(channel);
    // Note: we don't cleanup c-ares library: this will be done by libcurl
    //ares_library_cleanup();
    return (ipList.empty()) ? "" : ipList[rand() % ipList.size()];
  }

#else
#include <winsock2.h> // To get definition of gethostbyname, inet_ntoa, etc, use -lws2_32 for linking
  // Windows case: We use gethostbyname() to generate random ip
  string getRandomIP(const string &host_name) {
    static bool called = false;
    
    // Note: It's NOT safe to call gethostbyname() in multiple thread.
    boost::mutex::scoped_lock ipLock(getRandomIPMutex);
    if (called && last_host_name == host_name)
      return (ipList.empty()) ? "" : ipList[rand() % ipList.size()];

    //We are here => This function is called for the first time (or with a different
    // value of "host")
    called = true;
    last_host_name = host_name;
    ipList.clear();
    
    if (host_name.empty()){
      DXLOG(dx::logWARNING) << "getRandomIP() called with empty host ... will simply return empty string";
      return "";
    }
    struct hostent *he = gethostbyname(host_name.c_str());
    if (he == NULL) {
      DXLOG(dx::logWARNING) << "Call to gethostbyname failed. h_errno = '" << boost::lexical_cast<string>(h_errno) << "'";
      return "";
    }
    DXLOG(dx::logINFO) << "Found address(es) for host (real name): '" <<  he->h_name << "'";
    struct in_addr **addr_list = (struct in_addr **)he->h_addr_list;
    for(int i = 0; addr_list[i] != NULL; i++) {
      ipList.push_back(inet_ntoa(*addr_list[i]));
      DXLOG(dx::logINFO) << "\t" << i+1 << ". Pushing address '" << ipList[ipList.size() - 1] << "' to ipList";
    }
    
    if (ipList.empty())
      DXLOG(dx::logWARNING) << "The host '" << he->h_name << "' did not resolve to any ip address";

    return (ipList.empty()) ? "" : ipList[rand() % ipList.size()];
  }
#endif
