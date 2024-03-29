#!/usr/bin/env python3
#
# Copyright (C) 2013-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

import sys, json

preamble = '''// Do not modify this file by hand.
//
// It is automatically generated by src/api_wrappers/generateCppAPICCWrappers.py.
// (Run make api_wrappers to update it.)

#include "api.h"
namespace dx {'''

class_method_template = '''
  JSON {method_name}(const std::string &input_params, const bool safe_to_retry) {{
    return DXHTTPRequest("{route}", input_params, safe_to_retry);
  }}

  JSON {method_name}(const JSON &input_params, const bool safe_to_retry) {{{nonce_code}
    return {method_name}({input_params}.toString(), safe_to_retry);
  }}'''

object_method_template = '''
  JSON {method_name}(const std::string &object_id, const std::string &input_params, const bool safe_to_retry) {{
    return DXHTTPRequest(std::string("/") + object_id + std::string("/{method_route}"), input_params, safe_to_retry);
  }}

  JSON {method_name}(const std::string &object_id, const JSON &input_params, const bool safe_to_retry) {{{nonce_code}
    return {method_name}(object_id, {input_params}.toString(), safe_to_retry);
  }}'''

app_object_method_template = '''
  JSON {method_name}(const std::string &app_id_or_name, const std::string &input_params, const bool safe_to_retry) {{
    return DXHTTPRequest(std::string("/") + app_id_or_name + std::string("/{method_route}"), input_params, safe_to_retry);
  }}

  JSON {method_name}(const std::string &app_id_or_name, const JSON &input_params, const bool safe_to_retry) {{{nonce_code}
    return {method_name}(app_id_or_name, {input_params}.toString(), safe_to_retry);
  }}

  JSON {method_name}WithAlias(const std::string &app_name, const std::string &app_alias, const std::string &input_params, const bool safe_to_retry) {{
    return {method_name}(app_name + std::string("/") + app_alias, input_params, safe_to_retry);
  }}

  JSON {method_name}WithAlias(const std::string &app_name, const std::string &app_alias, const JSON &input_params, const bool safe_to_retry) {{{nonce_code}
    return {method_name}WithAlias(app_name, app_alias, {input_params}.toString(), safe_to_retry);
  }}'''

postscript = '''
}'''


def make_nonce_code(acceptNonce):
    return ("\n    JSON input_params_cp = Nonce::updateNonce(input_params);" if acceptNonce else "")


def make_input_params(accept_nonce):
    return ("input_params_cp" if accept_nonce else "input_params")


print(preamble)

for method in json.loads(sys.stdin.read()):
    route, signature, opts = method
    method_name = signature.split("(")[0]
    accept_nonce = True if 'acceptNonce' in opts else False
    if (opts['objectMethod']):
        root, oid_route, method_route = route.split("/")
        if oid_route == 'app-xxxx':
            print(app_object_method_template.format(method_name=method_name,
                                                    method_route=method_route,
                                                    nonce_code=make_nonce_code(accept_nonce),
                                                    input_params=make_input_params(accept_nonce)))
        else:
            print(object_method_template.format(method_name=method_name,
                                                method_route=method_route,
                                                nonce_code=make_nonce_code(accept_nonce),
                                                input_params=make_input_params(accept_nonce)))
    else:
        print(class_method_template.format(method_name=method_name,
                                           route=route,
                                           nonce_code=make_nonce_code(accept_nonce),
                                           input_params=make_input_params(accept_nonce)))

print(postscript)
