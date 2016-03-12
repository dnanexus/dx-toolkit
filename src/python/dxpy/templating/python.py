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

'''
Python templating code
'''
from __future__ import print_function, unicode_literals, division, absolute_import

from ..utils.printing import fill

def get_interpreter():
    return 'python2.7'

def get_path():
    return 'python'

class_to_dxclass = {
    "file": "dxpy.DXFile",
    "gtable": "dxpy.DXGTable",
    "record": "dxpy.DXRecord",
    "applet": "dxpy.DXApplet"
    }

def get_output_fmt(output_param):
    output_class = output_param["class"]
    output_fmt = ''
    if output_class.startswith('array'):
        item_class = output_class[6:]
        if item_class in class_to_dxclass:
            output_fmt = "[dxpy.dxlink(item) for item in " + output_param["name"] + "]"
        else:
            output_fmt = output_param["name"]
    elif output_class in class_to_dxclass:
        output_fmt = "dxpy.dxlink(" + output_param["name"] + ")"
    else:
        output_fmt = output_param["name"]
    return output_fmt

def add_init_input_lines(init_inputs, input_param, may_be_missing):
    if input_param["class"] in class_to_dxclass:
        init_str = "{name} = {dxclass}({name})".format(name=input_param["name"],
                                                       dxclass=class_to_dxclass[input_param["class"]])
    elif input_param["class"].startswith("array:") and input_param["class"][6:] in class_to_dxclass:
        init_str = "{name} = [{dxclass}(item) for item in {name}]".format(name=input_param["name"],
                                                                          dxclass=class_to_dxclass[input_param["class"][6:]])
    else:
        init_str = None

    if init_str is not None:
        if may_be_missing:
            init_inputs.append("if {name} is not None:".format(name=input_param['name']))
            indent = '    '
        else:
            indent = ''
        init_inputs.append(indent + init_str)

def get_strings(app_json,
                required_file_input_names, optional_file_input_names,
                required_file_array_input_names, optional_file_array_input_names,
                file_output_names, dummy_output_hash):
    input_sig_str = ''
    init_inputs_str = ''
    dl_files_str = ''
    ul_files_str = ''
    outputs_str = ''
    args_list = []
    kwargs_list = []
    init_inputs = []
    if "inputSpec" in app_json:
        # Iterate through input parameters and add them to the
        # signature and make their initialization lines
        for input_param in app_json["inputSpec"]:
            may_be_missing = input_param.get("optional") and "default" not in input_param
            if may_be_missing:
                # add as kwarg
                kwargs_list.append(input_param['name'])
            else:
                # argument that will always be present
                args_list.append(input_param["name"])

            # And no matter what, add line(s) for initializing it if present
            add_init_input_lines(init_inputs, input_param, may_be_missing)
        input_sig_str = ", ".join(args_list + [name + '=None' for name in kwargs_list])
    else:
        input_sig_str = "**kwargs"

    if init_inputs:
        init_inputs_str = '\n' + fill('The following line(s) initialize your data object inputs on the platform into dxpy.DXDataObject instances that you can start using immediately.', initial_indent='    # ', subsequent_indent='    # ', width=80)
        init_inputs_str += "\n\n    "
        init_inputs_str += "\n    ".join(init_inputs)
        init_inputs_str += "\n"

    if required_file_input_names or optional_file_input_names or \
       required_file_array_input_names or optional_file_array_input_names:
        dl_files_str = '\n' + fill('The following line(s) download your file inputs to the local file system using variable names for the filenames.', initial_indent='    # ', subsequent_indent='    # ', width=80) + '\n\n'
        if required_file_input_names:
            dl_files_str += "\n".join(['''    dxpy.download_dxfile({name}.get_id(), "{name}")
'''.format(name=name) for name in required_file_input_names])
        if optional_file_input_names:
            dl_files_str += "\n".join(['''    if {name} is not None:
        dxpy.download_dxfile({name}.get_id(), "{name}")
'''.format(name=name) for name in optional_file_input_names])
        if required_file_array_input_names:
            dl_files_str += "\n".join(['''    for i, f in enumerate({name}):
        dxpy.download_dxfile(f.get_id(), "{name}-" + str(i))
'''.format(name=name) for name in required_file_array_input_names])
        if optional_file_array_input_names:
            dl_files_str += "\n".join(['''    if {name} is not None:
        for i, f in enumerate({name}):
            dxpy.download_dxfile(f.get_id(), "{name}-" + str(i))
'''.format(name=name) for name in optional_file_array_input_names])

    if file_output_names:
        ul_files_str = "\n" + fill('''The following line(s) use the Python bindings to upload your file outputs after you have created them on the local file system.  It assumes that you have used the output field name for the filename for each output, but you can change that behavior to suit your needs.''', initial_indent="    # ", subsequent_indent="    # ", width=80)
        ul_files_str +='\n\n    '
        ul_files_str += "\n    ".join(['{name} = dxpy.upload_local_file("{name}")'.format(name=name) for name in file_output_names]) + '\n'

    if 'outputSpec' in app_json and app_json['outputSpec']:
        outputs_str = "    " + "\n    ".join(['output["{name}"] = {value}'.format(name=param["name"], value=get_output_fmt(param)) for param in app_json['outputSpec']]) + '\n'

    return input_sig_str, init_inputs_str, dl_files_str, ul_files_str, outputs_str
