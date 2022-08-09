import os
from dxpy.nextflow.nextflow_templates import get_nextflow_dxapp
from dxpy.nextflow.nextflow_templates import get_nextflow_src
import tempfile
import dxpy
import json



def write_exec(folder, content):
    exec_file = f"{folder}/nextflow.sh"
    os.makedirs(os.path.dirname(os.path.abspath(exec_file)), exist_ok=True)
    with open(exec_file, "w") as exec:
        exec.write(content)


def write_dxapp(folder, content):
    dxapp_file = f"{folder}/dxapp.json"
    with open(dxapp_file, "w") as dxapp:
        json.dump(content, dxapp)


'''
Creates files needed for nextflow applet build and returns folder with these files.
Note that folder is created as a tempfile
'''


def build_pipeline_from_repository(args):
    build_project_id = dxpy.WORKSPACE_ID
    if build_project_id is None:
        parser.error(
            "Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")
    input_hash = {
        "repository_url": args.repository,
        "repository_tag": args.tag,
        "config_profile": args.profile
    }

    api_options = {
        "name": "Nextflow build of %s" % (args.repository),
        "input": input_hash,
        "project": build_project_id,
    }

    # TODO: this will have to be an app app_run!
    app_run_result = dxpy.api.applet_run('applet-GFb8kQj0469zQ5P5BQGYpKJz', input_params=api_options)
    job_id = app_run_result["id"]
    if not args.brief:
        print("Started builder job %s" % (job_id,))
    dxpy.DXJob(job_id).wait_on_done(interval=1)
    applet_id, _ = dxpy.get_dxlink_ids(dxpy.api.job_describe(job_id)['output']['output_applet'])
    if not args.brief:
        print("Created Nextflow pipeline %s" % (applet_id))
    else:
        print(applet_id)
    return applet_id


def prepare_nextflow(resources_dir, args):
    assert os.path.exists(resources_dir)
    inputs = []
    dxapp_dir = tempfile.mkdtemp(prefix="dx.nextflow.")
    if os.path.exists(f"{resources_dir}/nextflow_schema.json"):
        inputs = prepare_inputs(f"{resources_dir}/nextflow_schema.json")
    DXAPP_CONTENT = get_nextflow_dxapp(inputs)
    EXEC_CONTENT = get_nextflow_src(inputs, args)
    write_dxapp(dxapp_dir, DXAPP_CONTENT)
    write_exec(dxapp_dir, EXEC_CONTENT)

    return dxapp_dir


def prepare_inputs(schema_file):
    def get_default_input_value(key):
        types = {
            "hidden": "false",
            "help": "Default help message"
            # TODO: add directory + file + path
        }
        if key in types:
            return types[key]
        return "NOT_IMPLEMENTED"

    def get_dx_type(nf_type):
        types = {
            "string": "str",
            "integer": "int",
            "number": "float",
            "boolean": "boolean",
            "object":"hash"  # TODO: check default values
        # TODO: add directory + file + path
        }
        if nf_type in types:
            return types[nf_type]
        return "string"
        # raise Exception(f"type {nf_type} is not supported by DNAnexus")

    inputs = []
    try:
        with open(schema_file, "r") as fh:
            schema = json.load(fh)
    except Exception as json_e:
        raise AssertionError(json_e)
    for d_key, d_schema in schema.get("definitions", {}).items():
        required_inputs = d_schema.get("required",[])
        for property_key, property in d_schema.get("properties", {}).items():
            dx_input = {}
            dx_input["name"] = f"{property_key}"
            dx_input["title"] = f"{property.get(property_key, dx_input['name'])}"
            dx_input["help"] = f"{property.get(property_key, get_default_input_value('help'))}"
            if property_key in property:
                dx_input["default"] = f"{property.get(property_key)}"
            dx_input["hidden"] = f"{property.get(property_key, get_default_input_value('hidden'))}"
            dx_input["class"] = f"{get_dx_type(property_key)}"
            if property_key not in required_inputs:
                dx_input["optional"] = True
            inputs.append(dx_input)
    return inputs
