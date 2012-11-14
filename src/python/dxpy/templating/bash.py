from dxpy.utils.printing import *

# bash templating code

def get_interpreter():
    return 'bash'

def get_path():
    return 'bash'

def get_output_fmt(output_class):
    output_fmt = ''
    base_class = output_class
    if output_class.startswith('array'):
        output_fmt = ' --array'
        base_class = output_class[6:]
    if base_class in ['int', 'float', 'string', 'boolean', 'hash']:
        output_fmt = base_class + output_fmt
    else:
        output_fmt = 'dxobject' + output_fmt
    return '--class ' + output_fmt

def get_strings(app_json, file_input_names, file_output_names, dummy_output_hash):
    init_inputs_str = ''
    dl_files_str = ''
    ul_files_str = ''
    outputs_str = ''
    if 'inputSpec' in app_json:
        init_inputs_str = "\n".join(["echo \"Value of {name}: '${name}'\"".format(name=input_param['name']) for input_param in app_json['inputSpec']])

    if len(file_input_names) > 0:
        dl_files_str = "\n" if init_inputs_str != '' else ""
        dl_files_str += fill('''The following line(s) use the dx command-line tool to download
your file inputs to the local file system using variable names for the filenames.
To recover the original filenames, you can use the output of "dx describe "$variable" --name".''',
                                    initial_indent='# ', subsequent_indent='# ', width=80) + '\n\n'
        dl_files_str += "\n".join(['dx download "$' + name + '" -o ' + name for name in file_input_names]) + '\n'
    if len(file_output_names) > 0:
        ul_files_str = "\n" if init_inputs_str != '' else ""
        ul_files_str += fill('''The following line(s) use the dx command-line tool to upload your file outputs after you have created them on the local file system.  It assumes that you have used the output field name for the filename for each output, but you can change that behavior to suit your needs.  Run "dx upload -h" to see more options to set metadata.''',
                             initial_indent='# ', subsequent_indent='# ', width=80) + '\n\n'
        ul_files_str += "\n".join(['{name}=$(dx upload {name} --brief)'.format(name=name) for name in file_output_names])

    if 'outputSpec' in app_json and len(app_json['outputSpec']) > 0:
        outputs_str = "\n" if (init_inputs_str != "" or dl_files_str != "") else ""
        outputs_str += fill('''The following line(s) use the utility dx-jobutil-add-output to format
and add output variables to your job's output as appropriate for the output class.  Run
\"dx-jobutil-add-output -h\" for more information on what it does.''',
                           initial_indent='# ', subsequent_indent='# ', width=80) + '\n\n'
        outputs_str += "\n".join(["dx-jobutil-add-output " + output_param['name'] + ' "$' + output_param['name'] + '" ' + get_output_fmt(output_param['class']) for output_param in app_json['outputSpec']])

    return '', init_inputs_str, dl_files_str, ul_files_str, outputs_str
