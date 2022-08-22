from dxpy.nextflow.nextflow_utils import get_template_dir
from dxpy.nextflow.nextflow_utils import get_source_file_name
import json
import os



def get_nextflow_dxapp(custom_inputs=[]):
    with open(os.path.join(str(get_template_dir()), 'dxapp.json'), 'r') as f:
        dxapp = json.load(f)
    dxapp["inputSpec"] = custom_inputs + dxapp["inputSpec"]
    return dxapp


# TODO:
def get_nextflow_src(inputs=[], profile=None):
    with open(os.path.join(str(get_template_dir()), get_source_file_name()), 'r') as f:
        src = f.read()

    run_inputs = ""
    for i in inputs:
        # wverride arguments that were not given at the runtime
        run_inputs = run_inputs + f'''
        if [ -n "${i['name']}" ]; then
            filtered_inputs="${{filtered_inputs}} --{i['name']}=${i['name']}"
        fi
        '''
    profile_arg = "-profile {}".format(profile) if profile else ""
    src = src.replace("@@RUN_INPUTS@@", run_inputs)
    src = src.replace("@@PROFILE_ARG@@", profile_arg)
    return src

# iterate through inputs of dxapp.json and add them here?
# put them in params file?