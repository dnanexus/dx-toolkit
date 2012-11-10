from dxpy.utils.printing import *

# Python templating code

def get_interpreter():
    return 'python2.7'

def get_path():
    return 'python'

class_to_dxclass = {
    "file": "DXFile",
    "gtable": "DXGTable",
    "record": "DXRecord",
    }

def get_strings(app_json, file_input_names, dummy_output_hash):
    input_sig_str = ''
    init_inputs_str = ''
    files_str = ''
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

        # Then, add keyword args
        for input_param in app_json["inputSpec"]:
            if ("optional" not in input_param or not input_param['optional']) and "default" not in input_param:
                continue
            if "default" in input_param:
                inputs.append("{name}={default}".format(name=input_param["name"], default=input_param["default"]))
            else:
                inputs.append("{name}=None".format(name=input_param["name"]))
        input_sig_str = ", ".join(inputs)
    else:
        input_sig_str = "**kwargs"

    if len(init_inputs) > 0:
        init_inputs_str = '\n' + fill('The following line(s) initialize your data object inputs on the platform into dxpy.DXDataObject instances that you can start using immediately.', initial_indent='    # ', subsequent_indent='    # ', width=80)
        init_inputs_str += "\n\n    "
        init_inputs_str += "\n    ".join(init_inputs)

    if len(file_input_names) > 0:
        files_str = '\n' if len(init_inputs) > 0 else ''
        files_str += fill('The following line(s) download your file inputs to the local file system using variable names for the filenames.', initial_indent='    # ', subsequent_indent='    # ', width=80)
        files_str += "\n\n    "
        files_str += "\n    ".join(['dxpy.download_dxfile(' + name + ', "' + name + '")' for name in file_input_names])
        files_str += "\n"

    outputs_str = str(dummy_output_hash)
    return input_sig_str, init_inputs_str, files_str, outputs_str
