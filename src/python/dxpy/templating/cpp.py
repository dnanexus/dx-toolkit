from dxpy.utils.printing import *

# C++ templating code

def get_interpreter():
    return 'bash'

def get_path():
    return 'cpp'

def get_strings(app_json, file_input_names, dummy_output_hash):
    init_inputs_str = ''
    files_str = ''
    outputs_str = ''
    inputs = []
    if 'inputSpec' in app_json and len(app_json['inputSpec']) > 0:
        init_inputs_str = '\n  '
        for input_param in app_json['inputSpec']:
            if ("optional" in input_param and input_param['optional']) or "default" in input_param:
                continue
            if input_param['class'] == 'int':
                inputs.append('int64_t {name} = input["{name}"].get<int64_t>();'.format(name=input_param['name']))
            elif input_param['class'] == 'float':
                inputs.append('double {name} = input["{name}"].get<double>();'.format(name=input_param['name']))
            elif input_param['class'] == 'hash':
                inputs.append('JSON {name} = input["{name}"];'.format(name=input_param['name']))
            elif input_param['class'] == 'boolean':
                inputs.append('bool {name} = input["{name}"].get<bool>();'.format(name=input_param['name']))
            elif input_param['class'] == 'string':
                inputs.append('string {name} = input["{name}"].get<string>();'.format(name=input_param['name']))
            elif input_param['class'] == 'gtable':
                inputs.append('DXGTable {name} = DXGTable(input["{name}"]["$dnanexus_link"].get<string>());'.format(name=input_param['name']))
            elif input_param['class'] == 'record':
                inputs.append('DXRecord {name} = DXRecord(input["{name}"]["$dnanexus_link"].get<string>());'.format(name=input_param['name']))
            elif input_param['class'] == 'file':
                inputs.append('DXFile {name} = DXFile(input["{name}"]["$dnanexus_link"].get<string>());'.format(name=input_param['name']))
        init_inputs_str += "\n  ".join(inputs)

    if len(file_input_names) > 0:
        files_str = "\n  " + "\n  ".join(['DXFile::downloadDXFile({name}.getID(), "{name}")'.format(name=fname) for fname in file_input_names]) + '\n'

    if len(dummy_output_hash) > 0:
        outputs_str = "\n  ".join(["JSON dummy_output = JSON(JSON_NULL);"] + \
                                      ["output[\"" + key + "\"] = dummy_output;" for key in dummy_output_hash.keys()])
    return '', init_inputs_str, files_str, outputs_str
