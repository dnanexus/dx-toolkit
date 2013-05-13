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

# C++ templating code

def get_interpreter():
    return 'bash'

def get_path():
    return 'cpp'

cpp_classes = {
    "int": "int64_t",
    "float": "double",
    "hash": "JSON",
    "boolean": "bool",
    "string": "string",
    "gtable": "DXGTable",
    "record": "DXRecord",
    "file": "DXFile"
    }

def format_input_var_class(var, classname, cpp_class):
    if classname in ["int", "float", "boolean", "string"]:
        return var + ".get<{cpp_class}>()".format(cpp_class=cpp_class)
    elif classname == "hash":
        return var
    elif classname in ["gtable", "record", "file"]:
        return "{cpp_class}({var})".format(var=var, cpp_class=cpp_class)

def get_input_fmt(input_param):
    name = input_param['name']
    is_array = input_param['class'].startswith('array:')
    classname = input_param['class'][6:] if is_array else input_param['class']
    cpp_class = cpp_classes[classname]

    init_str = ''
    if is_array:
        init_str += 'vector<{cpp_class}> {name};\n'.format(name=name, cpp_class=cpp_class)
        init_str += '  for (int i = 0; i < input["{name}"].size(); i++) {{'.format(name=name)
        init_str += '\n    {name}.push_back('.format(name=name)
        init_str += format_input_var_class('input["{name}"][i]'.format(name=name), classname, cpp_class)
        init_str += ');\n  }'
    else:
        init_str += '{cpp_class} {name} = '.format(name=name, cpp_class=cpp_class)
        init_str += format_input_var_class('input["{name}"]'.format(name=name), classname, cpp_class)
        init_str += ';'

    return init_str

def get_output_fmt(output_param):
    output_class = output_param["class"]
    output_fmt = ''
    if output_class.startswith('array'):
        output_fmt = "JSON(JSON_ARRAY)"
    elif output_class in ['int', 'float', 'string', 'boolean', 'hash']:
        output_fmt = output_param["name"]
    else:
        output_fmt = "DXLink({name}.getID())".format(name=output_param["name"])
    return output_fmt

def get_strings(app_json, file_input_names, file_array_input_names, file_output_names, dummy_output_hash):
    init_inputs_str = ''
    dl_files_str = ''
    ul_files_str = ''
    outputs_str = ''
    inputs = []
    if 'inputSpec' in app_json and len(app_json['inputSpec']) > 0:
        init_inputs_str = '\n  '
        for input_param in app_json['inputSpec']:
            if ("optional" in input_param and input_param['optional']) or "default" in input_param:
                continue
            inputs.append(get_input_fmt(input_param))
        init_inputs_str += "\n  ".join(inputs)
        init_inputs_str += "\n"

    if len(file_input_names) > 0 or len(file_array_input_names) > 0:
        dl_files_str = "\n" + fill('''The following line(s) use the C++ bindings to download your file inputs to the local file system using variable names for the filenames.  To recover the original filenames, you can use the output of "variable.describe()["name"].get<string>()".''', initial_indent="  // ", subsequent_indent="  // ") + "\n\n"
        if len(file_input_names) > 0:
            dl_files_str += "\n  ".join(['  DXFile::downloadDXFile({name}.getID(), "{name}");'.format(name=fname) for fname in file_input_names]) + "\n"
        if len(file_array_input_names) > 0:
            dl_files_str += "\n  ".join(['  for (int i = 0; i < {name}.size(); i++) {{\n    DXFile::downloadDXFile({name}[i].getID(), "{name}-" + {name}[i].getID());\n  }}'.format(name=fname) for fname in file_array_input_names]) + "\n"

    if len(file_output_names) > 0:
        ul_files_str = "\n" + fill('''The following line(s) use the C++ bindings to upload your file outputs after you have created them on the local file system.  It assumes that you have used the output field name for the filename for each output, but you can change that behavior to suit your needs.''', initial_indent="  // ", subsequent_indent="  // ")
        ul_files_str +='\n\n  '
        ul_files_str += "\n  ".join(['DXFile {name} = DXFile::uploadLocalFile("{name}");'.format(name=name) for name in file_output_names]) + '\n'

    if "outputSpec" in app_json and len(app_json['outputSpec']) > 0:
        outputs_str = "  " + "\n  ".join(["output[\"" + param["name"] + "\"] = " + get_output_fmt(param) + ";" for param in app_json["outputSpec"]]) + '\n'
    return '', init_inputs_str, dl_files_str, ul_files_str, outputs_str
