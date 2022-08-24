import os
import sys

from dxpy.nextflow.nextflow_templates import get_nextflow_dxapp
from dxpy.nextflow.nextflow_templates import get_nextflow_src
from dxpy.nextflow.nextflow_utils import get_template_dir
from dxpy.nextflow.nextflow_utils import write_exec
from dxpy.nextflow.nextflow_utils import write_dxapp
import dxpy
import json
from distutils.dir_util import copy_tree


def build_pipeline_from_repository(repository, tag, profile="", github_creds=, brief=False):
    """
    :param repository: URL to git repository
    :type repository: string
    :param tag: tag of given git repository. if not given, default branch is used.
    :type tag: string
    :param profile: Custom NF profile, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string
    :param github_creds: file containing github credentials for download private repositories
    :type github_creds: file
    :param brief: Level of verbosity
    :type brief: boolean

    Runs the Nextflow Pipeline Importer app, which creates NF applet from given repository.
    """
    # FIXME: is this already present somewhere?
    def create_dxlink(dx_object):
        try:
            if dxpy.is_dxlink(dx_object):
                return dx_object
            if ":" in dx_object:
                object_project, object_id = dx_object.split(":", 1)
            else:
                object_id = dx_object
                object_project = None
            if not dxpy.utils.resolver.is_hashid(object_id):
                object_project, _, object_id = dxpy.utils.resolver.resolve_existing_path(object_id, expected="entity", expected_classes=["file"], describe=False)
                object_id = object_id["id"]
            return dxpy.dxlink(object_id=object_id, project_id=object_project)
        except dxpy.utils.resolver.ResolutionError:
            print("GitHub credentials ('{}') file could not be found!".format(dx_object), file=sys.stderr)
            exit(2)


    build_project_id = dxpy.WORKSPACE_ID
    if build_project_id is None:
        parser.error(
            "Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")

    input_hash = {
        "repository_url": repository,
        "repository_tag": tag
    }
    if profile:
        input_hash["config_profile"] = profile
    if github_creds:
        input_hash["github_credentials"] = create_dxlink(github_creds)

    api_options = {
        "name": "Nextflow build of %s" % (repository),
        "input": input_hash,
        "project": build_project_id,
    }

    try:
        app_run_result = dxpy.api.app_run('app-nextflow_pipeline_importer', input_params=api_options)
    except dxpy.exceptions.ResourceNotFound:
        print("GitHub credentials file ('{}') could not be found!".format(github_creds), file=sys.stderr)
        raise
    job_id = app_run_result["id"]
    if not brief:
        print("Started builder job %s" % (job_id,))
    dxpy.DXJob(job_id).wait_on_done(interval=1)
    applet_id, _ = dxpy.get_dxlink_ids(dxpy.api.job_describe(job_id)['output']['output_applet'])
    if not brief:
        print("Created Nextflow pipeline %s" % (applet_id))
    else:
        print(applet_id)
    return applet_id

def prepare_nextflow(resources_dir, profile):
    """
    :param resources_dir: Directory with all resources needed for Nextflow Pipeline. Usually directory with user's NF files.
    :type resources_dir: str or Path
    :param profile: Custom NF profile, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string

    Creates files for creating applet, such as dxapp.json and source file. These files are created in temp directory.
    """
    assert os.path.exists(resources_dir)
    inputs = []
    os.makedirs(".dx.nextflow", exist_ok=True)
    dxapp_dir = os.path.join(resources_dir, '.dx.nextflow')
    if os.path.exists(f"{resources_dir}/nextflow_schema.json"):
        inputs = prepare_inputs(f"{resources_dir}/nextflow_schema.json")
    DXAPP_CONTENT = get_nextflow_dxapp(inputs)
    EXEC_CONTENT = get_nextflow_src(inputs=inputs, profile=profile)
    copy_tree(get_template_dir(), dxapp_dir)
    write_dxapp(dxapp_dir, DXAPP_CONTENT)
    write_exec(dxapp_dir, EXEC_CONTENT)
    return dxapp_dir


def prepare_inputs(schema_file):
    """
    :param schema_file: path to nextflow_schema.json file
    :type schema_file: str or Path

    Creates DNAnexus inputs from Nextflow inputs.
    """
    def get_default_input_value(key):
        types = {
            "hidden": False,
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
            "object": "hash"
            # TODO: add directory + file + path
        }
        if nf_type in types:
            return types[nf_type]
        # TODO: raise Exception after file+directory is implemented
        return "string"
        # raise Exception(f"type {nf_type} is not supported by DNAnexus")

    inputs = []
    try:
        with open(schema_file, "r") as fh:
            schema = json.load(fh)
    except Exception as json_e:
        raise AssertionError(json_e)
    for d_key, d_schema in schema.get("definitions", {}).items():
        required_inputs = d_schema.get("required", [])
        for property_key, property in d_schema.get("properties", {}).items():
            dx_input = {}
            dx_input["name"] = property_key
            dx_input["title"] = dx_input['name']
            dx_input["help"] = property.get('help_text', get_default_input_value('help'))
            if "default" in property:
                dx_input["default"] = property.get("default")
            dx_input["hidden"] = property.get('hidden', get_default_input_value('hidden'))
            dx_input["class"] = get_dx_type(property_key)
            if property_key not in required_inputs:
                dx_input["optional"] = True
                dx_input["help"] = "(Optional) {}".format(dx_input["help"])
            inputs.append(dx_input)
    return inputs
