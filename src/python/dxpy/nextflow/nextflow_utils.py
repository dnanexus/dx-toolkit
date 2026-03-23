#!/usr/bin/env python3
from os import path, makedirs, listdir
import re
import sys
import errno
import dxpy
import json
import shutil
import logging
from dxpy.exceptions import ResourceNotFound
from dxpy.nextflow.collect_images import collect_docker_images, bundle_docker_images


def _load_versions_manifest():
    """Load versions.json manifest."""
    manifest_path = path.join(path.dirname(dxpy.__file__), 'nextflow', 'versions.json')
    try:
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise dxpy.exceptions.DXCLIError(
            f"Failed to load Nextflow versions manifest at {manifest_path}: {e}")
    if "default" not in manifest or "versions" not in manifest:
        raise dxpy.exceptions.DXCLIError(
            "Malformed Nextflow versions manifest: missing 'default' or 'versions' key")
    required_keys = {"status", "nextflow_assets", "nextaur_assets", "awscli_assets"}
    for ver, cfg in manifest["versions"].items():
        missing = required_keys - set(cfg.keys())
        if missing:
            raise dxpy.exceptions.DXCLIError(
                f"Malformed version entry '{ver}': missing keys {sorted(missing)}")
    return manifest


def resolve_version(requested_version=None, warn=True):
    """Resolve Nextflow version. Returns (version_key, version_config).
    Prints deprecation warning to stderr if version is deprecated and warn=True.
    Raises DXError if version is not found.
    """
    manifest = _load_versions_manifest()
    if requested_version is None:
        requested_version = manifest["default"]
        if requested_version not in manifest["versions"]:
            raise dxpy.exceptions.DXCLIError(
                f"Nextflow versions manifest is misconfigured: default version '{requested_version}' "
                "is not listed in versions")
    version_config = manifest["versions"].get(requested_version)
    if version_config is None:
        available = ", ".join(sorted(manifest["versions"].keys()))
        raise dxpy.exceptions.DXCLIError(
            f"Nextflow version '{requested_version}' is not supported. "
            f"Available versions: {available}")
    if warn and version_config["status"] == "deprecated":
        sys.stderr.write(
            f"WARNING: Nextflow version {requested_version} is deprecated. "
            f"Consider upgrading to {manifest['default']}.\n")
    return requested_version, version_config


def get_source_file_name():
    return "src/nextflow.sh"


def get_resources_dir_name(resources_dir):
    """
    :param resources_dir: Directory with all source files needed to build an applet. Can be an absolute or a relative path.
    :type resources_dir: str or Path
    :returns: The name of the folder
    :rtype: str
    """
    if resources_dir == None:
        return ''
    return path.basename(path.abspath(resources_dir))


def get_resources_subpath(resources_dir):
    return path.join("/home/dnanexus/", get_resources_dir_name(resources_dir))


def get_importer_name():
    return "nextflow_pipeline_importer"


def get_template_dir():
    return path.join(path.dirname(dxpy.__file__), 'templating', 'templates', 'nextflow')


def get_project_with_assets(region):
    nextflow_basepath = path.join(path.dirname(dxpy.__file__), 'nextflow')
    projects_path = path.join(nextflow_basepath, "app_asset_projects_ids_prod.json")

    try:
        with open(projects_path, 'r') as projects_f:
            project = json.load(projects_f)[region]
            dxpy.describe(project, fields={})  # existence check
    except ResourceNotFound:
        projects_path = path.join(nextflow_basepath, "app_asset_projects_ids_staging.json")
        with open(projects_path, 'r') as projects_f:
            project = json.load(projects_f)[region]

    return project


def is_importer_job():
    try:
        with open("/home/dnanexus/dnanexus-job.json", "r") as f:
            job_info = json.load(f)
            return job_info.get("executableName") == get_importer_name()
    except Exception:
        return False


def write_exec(folder, content):
    exec_file = "{}/{}".format(folder, get_source_file_name())
    try:
        makedirs(path.dirname(path.abspath(exec_file)))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
        pass
    with open(exec_file, "w") as fh:
        fh.write(content)


def find_readme(dir):
    """
    Returns first readme (in alphabetical order) from a root of a given folder
    :param dir: Directory in which we search for readme files
    :type dir: str or Path
    :returns: List[str]
    """
    readme_pattern = re.compile(r"readme(\.(txt|md|rst|adoc|html|txt|asciidoc|org|text|textile|pod|wiki))?", re.IGNORECASE)
    file_list = [f for f in listdir(dir) if path.isfile(path.join(dir, f))]
    readme_files = [file for file in file_list if readme_pattern.match(file)]
    readme_files.sort()
    return readme_files[0] if readme_files else None


def create_readme(source_dir, destination_dir):
    """
    :param destination_dir: Directory where readme is going to be created
    :type destination_dir: str or Path
    :param source_dir: Directory from which readme is going to be copied
    :type source_dir: str or Path
    :returns: None
    """
    readme_file = find_readme(source_dir)

    if readme_file:
        source_path = path.join(source_dir, readme_file)
        destination_path = path.join(destination_dir, "Readme.md")
        shutil.copy2(source_path, destination_path)


def write_dxapp(folder, content):
    dxapp_file = "{}/dxapp.json".format(folder)
    with open(dxapp_file, "w") as dxapp:
        json.dump(content, dxapp)


def get_regional_options(region, resources_dir, profile, cache_docker, nextflow_pipeline_params, version_config=None):
    nextaur_asset, nextflow_asset, awscli_asset = get_nextflow_assets(region, version_config=version_config)
    regional_instance_type = get_instance_type(region)
    if cache_docker:
        image_refs = collect_docker_images(resources_dir, profile, nextflow_pipeline_params)
        image_bundled = bundle_docker_images(image_refs)
    else:
        image_bundled = {}

    project_with_assets = get_project_with_assets(region)
    regional_options = {
        region: {
            "systemRequirements": {
                "*": {
                    "instanceType": regional_instance_type
                }
            },
            "assetDepends": [
                {"id": {"$dnanexus_link": {
                    "id": nextaur_asset,
                    "project": project_with_assets
                }}},
                {"id": {"$dnanexus_link": {
                    "id": nextflow_asset,
                    "project": project_with_assets
                }}},
                {"id": {"$dnanexus_link": {
                    "id": awscli_asset,
                    "project": project_with_assets
                }}}
            ],
            "bundledDepends": image_bundled
        }
    }
    return regional_options


def get_instance_type(region):
    json_file_path = path.join(path.dirname(dxpy.__file__), 'nextflow', 'default_nextflow_instance_types.json')
    try:
        with open(json_file_path, 'r') as f:
            instance_type_mapping = json.load(f)
    except FileNotFoundError:
        raise dxpy.exceptions.ResourceNotFound(f"Instance types file not found at {json_file_path}.")

    instance_type = instance_type_mapping.get(region)
    if not instance_type:
        raise dxpy.exceptions.ResourceNotFound("Instance type is not specified for region {}.".format(region))
    return instance_type


def get_nextflow_assets(region, nextflow_version=None, version_config=None):
    if nextflow_version is not None and version_config is not None:
        raise dxpy.exceptions.DXCLIError("Specify nextflow_version or version_config, not both")
    if version_config is None:
        _, version_config = resolve_version(nextflow_version)
    nextflow_basepath = path.join(path.dirname(dxpy.__file__), 'nextflow')

    nextaur_filename = version_config["nextaur_assets"]
    nextflow_filename = version_config["nextflow_assets"]
    awscli_filename = version_config["awscli_assets"]

    nextaur_assets = path.join(nextflow_basepath, nextaur_filename)
    nextflow_assets = path.join(nextflow_basepath, nextflow_filename)
    awscli_assets = path.join(nextflow_basepath, awscli_filename)

    def _load_regional_assets(nextaur_path, nextflow_path, awscli_path):
        try:
            with open(nextaur_path, 'r') as nextaur_f, open(nextflow_path, 'r') as nextflow_f, open(awscli_path, 'r') as awscli_f:
                return json.load(nextaur_f)[region], json.load(nextflow_f)[region], json.load(awscli_f)[region]
        except KeyError:
            raise dxpy.exceptions.DXCLIError(
                f"Nextflow assets not available for region '{region}'. "
                "Check that your Nextflow version supports this region.")
        except json.JSONDecodeError as e:
            raise dxpy.exceptions.DXCLIError(
                f"Malformed Nextflow asset file: {e}")

    try:
        nextaur, nextflow, awscli = _load_regional_assets(nextaur_assets, nextflow_assets, awscli_assets)
        dxpy.describe(nextflow, fields={})  # existence check
        return nextaur, nextflow, awscli
    except (ResourceNotFound, FileNotFoundError):
        nextaur_staging = path.join(nextflow_basepath, nextaur_filename.replace(".json", ".staging.json"))
        nextflow_staging = path.join(nextflow_basepath, nextflow_filename.replace(".json", ".staging.json"))
        awscli_staging = path.join(nextflow_basepath, awscli_filename.replace(".json", ".staging.json"))
        try:
            return _load_regional_assets(nextaur_staging, nextflow_staging, awscli_staging)
        except FileNotFoundError:
            raise dxpy.exceptions.DXCLIError(
                "Staging asset files not found for the resolved Nextflow version. "
                f"Expected files like: {nextaur_staging}")


def get_nested(args, arg_path):
    """
    :param args: extra args from command input
    :type args: dict
    :param arg_path: list of a dxapp.json location of an allowed extra_arg (eg. ["runSpec", "timeoutPolicy"])
    :type arg_path: tuple/list
    :returns: nested arg value if it exists in args, otherwise None
    """
    for key in arg_path:
        if not isinstance(args, dict):
            return None
        args = args.get(key)
        if args is None:
            return None
    return args


def get_allowed_extra_fields_mapping():
    """
    :returns: tuple (arg_path, target_key)
        arg_path is a list of a dxapp.json location of an allowed extra_arg, target_key is name of an argument for a remote build
    """
    return [
        (["name"], "name"),
        (["title"], "title"),
        (["summary"], "summary"),
        (["runSpec", "timeoutPolicy"], "timeout_policy"),
        (["runSpec", "release"], "release"),
        (["details", "whatsNew"], "whats_new"),
    ]

