#ifndef DXCPP_EXEC_UTILS_H
#define DXCPP_EXEC_UTILS_H

#include <string>

#include "dxjson/dxjson.h"

/**
 * This function reads the local file job_input.json and loads the
 * JSON contents into the given JSON variable.
 *
 * @param input JSON variable to be set
 */
void loadInput(dx::JSON &input);

/**
 * This function serializes the given JSON variable and saves it to
 * the local file job_output.json.
 *
 * @param output JSON variable to be serialized
 */
void writeOutput(const dx::JSON &output);

/**
 * This function records the given error message into the local file
 * job_error.json and exits with a nonzero exit code.
 *
 * @param message Error message which will be shown to the user
 * @param internal Whether the error should be reported as an
 * AppInternalError instead of AppError
 */
void reportError(const std::string &message, const bool internal=false);

#endif
