#ifndef __DXCPP_UTILS_H__
#define __DXCPP_UTILS_H__

#include <string>
#include <vector>

/** @internal
 */

std::string getUserHomeDirectory();

std::string joinPath(const std::string &first_path,
                     const std::string &second_path,
                     const std::string &third_path="");

// Returns MD5 hash (as a hex string) for the given vector<char>
std::string getHexifiedMD5(const std::vector<char> &inp);
std::string getHexifiedMD5(const unsigned char *ptr, const unsigned long size);
std::string getHexifiedMD5(const std::string &inp);

#endif
