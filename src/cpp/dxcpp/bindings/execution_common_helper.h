#ifndef __EXECUTION_COMMON_HELPER_H__
#define __EXECUTION_COMMON_HELPER_H__
#include "dxjson/dxjson.h"
#include "../exceptions.h"
/** @internal
 */
void appendDependsOnAndInstanceType(dx::JSON &input, 
                                    const std::vector<std::string> &depends_on=std::vector<std::string>(),
                                    const std::string fn_name="main",
                                    const dx::JSON &instance_type=dx::JSON(dx::JSON_NULL));
#endif
