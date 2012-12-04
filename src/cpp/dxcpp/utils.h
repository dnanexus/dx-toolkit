#ifndef __DXCPP_UTILS_H__
#define __DXCPP_UTILS_H__

#include <string>

/** @internal
 */

std::string getUserHomeDirectory();

std::string joinPath(const std::string &first_path,
                     const std::string &second_path,
                     const std::string &third_path="");

#endif
