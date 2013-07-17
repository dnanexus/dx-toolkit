# Copyright (C) 2013 DNAnexus, Inc.
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

from dxpy.utils.printing import fill

# Python templating code

def get_interpreter():
    return 'python2.7'

def get_path():
    return 'python'

class_to_dxclass = {
    "file": "dxpy.DXFile",
    "gtable": "dxpy.DXGTable",
    "record": "dxpy.DXRecord",
    }

def get_output_fmt(output_param):
    output_class = output_param["class"]
    output_fmt = ''
    if output_class.startswith('array'):
        output_fmt = '[]'
    elif output_class in ['int', 'float', 'string', 'boolean', 'hash']:
        output_fmt = output_param["name"]
    else:
        output_fmt = "dxpy.dxlink(" + output_param["name"] + ")"
    return output_fmt

def get_strings(app_json, file_input_names, file_array_input_names, file_output_names, dummy_output_hash):
    input_sig_str = ''
    init_inputs_str = ''
    dl_files_str = ''
    ul_files_str = ''
    outputs_str = ''
    inputs = []
    init_inputs = []
    if "inputSpec" in app_json:
        # First, add all non-keyword args
        for input_param in app_json["inputSpec"]:
            if ("optional" in input_param and input_param['optional']) or "default" in input_param:
                continue
            inputs.append(input_param["name"])
            if input_param["class"] in class_to_dxclass:
                init_inputs.append("{name} = {dxclass}({name})".format(name=input_param["name"],
                                                                       dxclass=class_to_dxclass[input_param["class"]]))
            elif input_param["class"].startswith("array:") and input_param["class"][6:] in class_to_dxclass:
                init_inputs.append("{name} = [{dxclass}(item) for item in {name}]".format(name=input_param["name"],
                                                                                          dxclass=class_to_dxclass[input_param["class"][6:]]))

        # Then, add keyword args
        for input_param in app_json["inputSpec"]:
            if ("optional" not in input_param or not input_param['optional']) and "default" not in input_param:
                continue
            if "default" in input_param:
                inputs.append("{name}={default}".format(name=input_param["name"], default=(input_param["default"] if input_param['class'] != 'string' else '"' + input_param['default'] + '"')))
            else:
                inputs.append("{name}=None".format(name=input_param["name"]))
        input_sig_str = ", ".join(inputs)
    else:
        input_sig_str = "**kwargs"

    if len(init_inputs) > 0:
        init_inputs_str = '\n' + fill('The following line(s) initialize your data object inputs on the platform into dxpy.DXDataObject instances that you can start using immediately.', initial_indent='    # ', subsequent_indent='    # ', width=80)
        init_inputs_str += "\n\n    "
        init_inputs_str += "\n    ".join(init_inputs)
        init_inputs_str += "\n"

    if len(file_input_names) > 0 or len(file_array_input_names) > 0:
        dl_files_str = '\n' + fill('The following line(s) download your file inputs to the local file system using variable names for the filenames.', initial_indent='    # ', subsequent_indent='    # ', width=80) + '\n\n'
        if len(file_input_names) > 0:
            dl_files_str += "\n".join(['    dxpy.download_dxfile(' + name + '.get_id(), "' + name + '")' for name in file_input_names]) + "\n"
        if len(file_array_input_names) > 0:
            dl_files_str += "\n".join(['    for i in range(len({name})):\n        dxpy.download_dxfile({name}[i].get_id(), "{name}-" + str(i))'.format(name=name) for name in file_array_input_names]) + "\n"

    if len(file_output_names) > 0:
        ul_files_str = "\n" + fill('''The following line(s) use the Python bindings to upload your file outputs after you have created them on the local file system.  It assumes that you have used the output field name for the filename for each output, but you can change that behavior to suit your needs.''', initial_indent="    # ", subsequent_indent="    # ", width=80)
        ul_files_str +='\n\n    '
        ul_files_str += "\n    ".join(['{name} = dxpy.upload_local_file("{name}")'.format(name=name) for name in file_output_names]) + '\n'

    if 'outputSpec' in app_json and len(app_json['outputSpec']) > 0:
        outputs_str = "    " + "\n    ".join(['output["{name}"] = {value}'.format(name=param["name"], value=get_output_fmt(param)) for param in app_json['outputSpec']]) + '\n'

    return input_sig_str, init_inputs_str, dl_files_str, ul_files_str, outputs_str
