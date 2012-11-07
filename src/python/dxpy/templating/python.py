from dxpy.utils.printing import *

# Python templating code

def get_interpreter():
    return 'python2.7'

def get_path():
    return 'python'

def get_strings(app_json, file_input_names, dummy_output_hash):
    inputs_str = ''
    files_str = ''
    outputs_str = ''
    inputs = []
    if "inputSpec" in app_json:
        # First, add all non-keyword args
        for input_param in app_json["inputSpec"]:
            if ("optional" in input_param and input_param['optional']) or "default" in input_param:
                continue
            inputs.append(input_param["name"])

        # Then, add keyword args
        for input_param in app_json["inputSpec"]:
            if ("optional" not in input_param or not input_param['optional']) and "default" not in input_param:
                continue
            if "default" in input_param:
                inputs.append("{name}={default}".format(name=input_param["name"], default=input_param["default"]))
            else:
                inputs.append("{name}=None".format(name=input_param["name"]))
        inputs_str = ", ".join(inputs)
    else:
        inputs_str = "**kwargs"

    if len(file_input_names) > 0:
        files_str = '\n' + fill('The following line(s) download your file inputs to the local file system using variable names for the filenames.', initial_indent='    # ', subsequent_indent='    # ', width=80)
        files_str += "\n\n    "
        files_str += "\n    ".join(['dxpy.download_dxfile(' + name + ', "' + name + '")' for name in file_input_names])
        files_str += "\n"

    outputs_str = str(dummy_output_hash)
    return inputs_str, files_str, outputs_str
