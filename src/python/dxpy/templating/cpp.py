from dxpy.utils.printing import *

# C++ templating code

def get_interpreter():
    return 'bash'

def get_path():
    return 'cpp'

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

def get_strings(app_json, file_input_names, file_output_names, dummy_output_hash):
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
                inputs.append('DXGTable {name} = DXGTable(input["{name}"]);'.format(name=input_param['name']))
            elif input_param['class'] == 'record':
                inputs.append('DXRecord {name} = DXRecord(input["{name}"]);'.format(name=input_param['name']))
            elif input_param['class'] == 'file':
                inputs.append('DXFile {name} = DXFile(input["{name}"]);'.format(name=input_param['name']))
        init_inputs_str += "\n  ".join(inputs)

    if len(file_input_names) > 0:
        dl_files_str = "\n" + fill('''The following line(s) use the C++ bindings to download your file inputs to the local file system using variable names for the filenames.  To recover the original filenames, you can use the output of "variable.describe()["name"].get<string>()".''', initial_indent="  // ", subsequent_indent="  // ")
        dl_files_str += '\n\n  '
        dl_files_str += "\n  ".join(['DXFile::downloadDXFile({name}.getID(), "{name}");'.format(name=fname) for fname in file_input_names]) + "\n"

    if len(file_output_names) > 0:
        ul_files_str = "\n" + fill('''The following line(s) use the C++ bindings to upload your file outputs after you have created them on the local file system.  It assumes that you have used the output field name for the filename for each output, but you can change that behavior to suit your needs.''', initial_indent="  // ", subsequent_indent="  // ")
        ul_files_str +='\n\n  '
        ul_files_str += "\n  ".join(['DXFile {name} = DXFile::uploadLocalFile("{name}");'.format(name=name) for name in file_output_names]) + '\n'

    if "outputSpec" in app_json and len(app_json['outputSpec']) > 0:
        outputs_str = "  " + "\n  ".join(["output[\"" + param["name"] + "\"] = " + get_output_fmt(param) + ";" for param in app_json["outputSpec"]]) + '\n'
    return '', init_inputs_str, dl_files_str, ul_files_str, outputs_str
