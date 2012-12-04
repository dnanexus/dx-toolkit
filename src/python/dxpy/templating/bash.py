from dxpy.utils.printing import *

# bash templating code

def get_interpreter():
    return 'bash'

def get_path():
    return 'bash'

def get_strings(app_json, file_input_names, file_array_input_names, file_output_names, dummy_output_hash):
    init_inputs_str = ''
    dl_files_str = ''
    ul_files_str = ''
    outputs_str = ''

    if 'inputSpec' in app_json and len(app_json['inputSpec']) > 0:
        init_inputs_str = "\n" + "\n".join(["    echo \"Value of {name}: '${var}'\"".format(name=input_param['name'], var=(("{" + input_param['name'] + "[@]}") if input_param['class'].startswith('array:') else input_param['name'])) for input_param in app_json['inputSpec']]) + "\n"

    if len(file_input_names) > 0 or len(file_array_input_names) > 0:
        dl_files_str = "\n" + fill('''The following line(s) use the dx command-line tool to download
your file inputs to the local file system using variable names for the filenames.
To recover the original filenames, you can use the output of "dx describe "$variable" --name".''',
                                    initial_indent='    # ', subsequent_indent='    # ', width=80) + '\n\n'
        if len(file_input_names) > 0:
            dl_files_str += "\n".join(['    dx download "$' + name + '" -o ' + name for name in file_input_names]) + '\n'
        if len(file_array_input_names) > 0:
            dl_files_str += "\n".join(['    for i in ${!' + name + '[@]}\n    do\n        dx download "${' + name + '[$i]}" -o ' + name + '-$i\n    done' for name in file_array_input_names]) + '\n'

    if len(file_output_names) > 0:
        ul_files_str = "\n" if init_inputs_str != '' else ""
        ul_files_str += fill('''The following line(s) use the dx command-line tool to upload your file outputs after you have created them on the local file system.  It assumes that you have used the output field name for the filename for each output, but you can change that behavior to suit your needs.  Run "dx upload -h" to see more options to set metadata.''',
                             initial_indent='    # ', subsequent_indent='    # ', width=80) + '\n\n'
        ul_files_str += "\n".join(['    {name}=$(dx upload {name} --brief)'.format(name=name) for name in file_output_names]) + "\n"

    if 'outputSpec' in app_json and len(app_json['outputSpec']) > 0:
        outputs_str = "\n" + fill('''The following line(s) use the utility dx-jobutil-add-output to format
and add output variables to your job's output as appropriate for the output class.  Run
\"dx-jobutil-add-output -h\" for more information on what it does.''',
                           initial_indent='    # ', subsequent_indent='    # ', width=80) + '\n\n'
        outputs_str += "\n".join(["    dx-jobutil-add-output " + output_param['name'] + ' "$' + output_param['name'] + '" --class=' + output_param['class'] for output_param in app_json['outputSpec']])
    elif 'outputSpec' not in app_json:
        outputs_str = "\n" + fill('''No output spec is specified, but
if you would like to add output fields, you can add invocations of the
dx-jobutil-add-output utility to format and add values to the
job_output.json file.  For example, "dx-jobutil-add-output keyname 32"
will add an output field called "keyname" with value equal to the
number 32.  Run \"dx-jobutil-add-output -h\" for more details on what
it does.''',
                                  initial_indent='    # ', subsequent_indent='    # ', width=80)

    return '', init_inputs_str, dl_files_str, ul_files_str, outputs_str
