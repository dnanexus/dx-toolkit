#include <string>
#include <boost/thread.hpp>

// The variable below are "extern" because main.cpp also uses them
extern bool forceRefreshDNS;
extern boost::mutex forceRefreshDNSMutex;

// Get a random ip for the given host name
// Returns empty string if ip address could not be found/host couldn't be resolved
// 
// This function is defined differently for both WINDOWS_BUILD and !WINDOWS_BUILD case using #if guards
std::string getRandomIP(const std::string &host_name);
