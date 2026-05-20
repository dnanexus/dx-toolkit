#!/usr/bin/env python3
import os
import sys
import dxpy
import json
import argparse
import logging
from glob import glob
import shutil
import tempfile
from functools import partial

from dxpy.nextflow.nextflow_templates import (get_nextflow_dxapp, get_nextflow_src)
from dxpy.nextflow.nextflow_utils import (get_template_dir, write_exec, write_dxapp, get_importer_name, get_importer_object,
                                          create_readme, get_nested, get_allowed_extra_fields_mapping,
                                          resolve_version)
from dxpy.nextflow.collect_images import scan_ecr_floating_tags_in_config
from dxpy.cli.exec_io import parse_obj
from dxpy.cli import try_call
from dxpy.utils.resolver import resolve_existing_path


parser = argparse.ArgumentParser(description="Uploads a DNAnexus App.")


def _npi_supports_version_selection():
    """Check if deployed NPI app accepts nextflow_version input."""
    try:
        npi = get_importer_object()
        desc = npi.describe(fields={"inputSpec": True})
        input_names = {inp["name"] for inp in desc.get("inputSpec", [])}
        return "nextflow_version" in input_names
    except (dxpy.exceptions.ResourceNotFound, dxpy.exceptions.DXAPIError) as e:
        logging.debug("Could not check NPI version support: %s", e)
        return False


# TODO(APPS-3915): Once the NPI version that declares ecr_role_arn_to_assume,
# ecr_job_token_audience, and ecr_job_token_subject_claims is the minimum deployed
# version, remove _npi_input_names(), _ECR_SPECIFIC_INPUTS,
# preflight_validate_for_cache_docker(), and the ECR slot check.
# At that point build_pipeline_with_npi() forwards the three ECR auth fields
# unconditionally without needing to check the NPI input spec first.
# Also delete src/python/test/test_nextflow_builder_npi_gate.py.
def _npi_input_names():
    """Return the set of input names the deployed NPI app accepts.

    Used to gate forwarding of new optional input fields (ECR auth fields)
    so an older NPI that does not yet declare them
    will not reject the launch with InvalidInput. Returns ``None`` if the
    deployed app cannot be described, signalling the caller to skip
    forwarding any input that isn't part of the historically-stable set.
    """
    try:
        npi = get_importer_object()
        desc = npi.describe(fields={"inputSpec": True})
        return {inp["name"] for inp in desc.get("inputSpec", [])}
    except (dxpy.exceptions.ResourceNotFound, dxpy.exceptions.DXAPIError) as e:
        logging.debug("Could not describe NPI input spec: %s", e)
        return None


# Names of the ECR-specific input fields declared by the NPI.  Used by the
# preflight ECR slot check to fail fast when the deployed NPI predates ECR support.
_ECR_SPECIFIC_INPUTS = frozenset({
    "ecr_role_arn_to_assume",
    "ecr_job_token_audience",
    "ecr_job_token_subject_claims",
})


def preflight_validate_for_cache_docker(src_dir, profile=None, ecr_role_arn=None):
    """Pre-upload validation for `dx build --nextflow --cache-docker`.

    Called from dx_build_app.py BEFORE the local pipeline source is uploaded
    to the user's project. Any DXError raised here aborts the build cleanly
    without leaving an orphaned `.nf_source/` upload behind that the user
    would otherwise have to `dx rm -r` manually before retrying.

    Parameters
    ----------
    src_dir : str or None
        Path to the local Nextflow pipeline directory, or None when building
        from a remote Git repository (``--repository <url>`` mode).
    profile : str or None
        Active Nextflow profile (``--profile``). Used by the floating-tag
        scanner to check profile-specific container overrides.
    ecr_role_arn : str or None
        IAM role ARN passed via ``--ecr-role-arn``.  When set, signals that
        the NPI job must authenticate to ECR to pull images, which triggers:
          1. A check that the deployed NPI declares the ECR input slots.
          2. A floating-tag guard — ECR images with ``:latest`` or no tag
             cannot be reliably cached (see note in collect_images.py).

        NOTE: This is the build-time-only ECR role.  It is distinct from
        ``dnanexus.ecrRoleArnToAssume`` in nextflow.config, which drives
        runtime ECR auth on every task subjob.  Keeping the two roles
        separate means the cached applet has zero ECR dependency at runtime,
        even if the private registry later revokes access.

    Additional tightening over the in-build gate:
      - When the deployed NPI cannot be described, raise unconditionally.
      - When ``ecr_role_arn`` is provided, require the NPI to declare the
        ECR input slots — otherwise the importer job will silently fall back
        to anonymous pulls and fail at the docker-pull step.
    """
    accepted = _npi_input_names()
    if accepted is None:
        raise dxpy.exceptions.DXError(
            "Could not describe the deployed Nextflow Pipeline Importer. "
            "Refusing to launch a --cache-docker build before verifying "
            "the importer accepts the expected input fields. Verify the "
            "importer app is deployed and you have describe permission."
        )

    if ecr_role_arn:
        # Floating-tag guard: only applicable to local src_dir mode (no floating
        # tags in --repository <url> mode since there is no local config to scan).
        # Runs BEFORE the ECR slot check so user config errors are surfaced first,
        # regardless of whether the deployed NPI has been upgraded to declare ECR
        # input slots.  A floating-tag rejection is always a user error that must
        # be fixed before retrying; failing early here also prevents an orphaned
        # .nf_source/ upload on a guard rejection.
        #
        # NOT driven by nextflow.config — the runtime ECR config (ecrRoleArnToAssume
        # in nextflow.config) is irrelevant here; it drives runtime auth, not
        # this build-time caching step.
        if src_dir:
            floating = scan_ecr_floating_tags_in_config(src_dir, profile=profile)
            if floating:
                raise dxpy.exceptions.DXError(
                    "ECR container(s) {floating} use a floating tag (latest or "
                    "no tag) and cannot be reliably cached. Nextaur resolves "
                    "the digest of floating-tag images via `docker manifest "
                    "inspect` on the head job, where the Docker daemon is not "
                    "authenticated to ECR — so the pre-cached image would be "
                    "silently bypassed on every run.\n"
                    "Pin the image to an explicit tag or digest "
                    "(e.g. myrepo:1.2 or myrepo@sha256:...) "
                    "before building with --cache-docker.".format(
                        floating=sorted(floating)
                    )
                )

        # Fail fast if the deployed NPI does not declare ECR input slots.
        # Without these slots, the NPI job will not receive the role ARN and
        # will fall back to anonymous docker pulls, failing opaquely several
        # minutes into the build job.  The floating-tag guard runs first so
        # user config errors are surfaced before this infrastructure check.
        # This check applies to both src_dir and --repository <url> modes.
        missing = sorted(_ECR_SPECIFIC_INPUTS - set(accepted))
        if missing:
            raise dxpy.exceptions.DXError(
                "The deployed Nextflow Pipeline Importer does not declare "
                "input(s) {missing}. --cache-docker with --ecr-role-arn "
                "requires an importer that supports private ECR registries. "
                "Upgrade the importer app or set DX_NPI_NAME to a build "
                "that declares these inputs.".format(missing=missing)
            )

    if src_dir is None:
        # --repository <url> mode: no local config to scan beyond what is above.
        return


def build_pipeline_with_npi(
        repository=None,
        tag=None,
        cache_docker=False,
        docker_secrets=None,
        nextflow_pipeline_params="",
        profile="",
        git_creds=None,
        brief=False,
        destination=None,
        extra_args=None,
        nextflow_version=None,
        src_dir=None,
        ecr_role_arn=None,
        ecr_job_token_audience=None,
        ecr_job_token_subject_claims=None,
):
    """
    :param repository: URL to a Git repository
    :type repository: string
    :param tag: tag of a given Git repository. If it is not provided, the default branch is used.
    :type tag: string
    :param cache_docker: Pull a remote docker image and store it on the platform
    :type cache_docker: bool
    :param docker_secrets: Dx file id with the private docker registry credentials
    :type docker_secrets: string
    :param nextflow_pipeline_params: Custom Nextflow pipeline parameters
    :type nextflow_pipeline_params: string
    :param profile: Custom Nextflow profile, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string
    :param brief: Level of verbosity
    :type brief: boolean
    :param ecr_role_arn: IAM role ARN for build-time ECR auth (--ecr-role-arn CLI flag).
        Forwarded as an explicit NPI input.  NOT read from nextflow.config —
        see design note in dx.py and ECR_Private_Registry.md.
    :type ecr_role_arn: str or None
    :param ecr_job_token_audience: OIDC audience for the build-time ECR role.
    :type ecr_job_token_audience: str or None
    :param ecr_job_token_subject_claims: OIDC subject claims for the build-time ECR role.
    :type ecr_job_token_subject_claims: str or None
    :returns: ID of the created applet

    Runs the Nextflow Pipeline Importer app, which creates a Nextflow applet from a given Git repository.
    """

    def parse_extra_args(args):
        """
        :param args: extra args from command input
        :returns: overridable fields from extra_args
        """
        return {
            target_key: val
            for arg_path, target_key in get_allowed_extra_fields_mapping()
            if (val := get_nested(args, arg_path)) is not None
        }

    extra_args = extra_args or {}
    build_project_id = dxpy.WORKSPACE_ID
    build_folder = None
    input_hash = parse_extra_args(extra_args)
    input_hash["repository_url"] = repository

    # { NPI_input_name: (raw value, transformation function),...}
    input_updates = {
        "repository_tag": (tag, None),
        "config_profile": (profile, None),
        "cache_docker": (cache_docker, None),
        "nextflow_pipeline_params": (nextflow_pipeline_params, None),
        "docker_secrets": (docker_secrets, partial(parse_obj, klass="file")),
        "github_credentials": (git_creds, partial(parse_obj, klass="file")),
    }
    for key, (raw_value, transform) in input_updates.items():
        if raw_value:
            input_hash[key] = transform(raw_value) if transform else raw_value

    # Forward build-time ECR credentials from explicit CLI flags.
    # These are NEVER read from nextflow.config so they are never bundled
    # into the resulting applet. After images are cached, runtime executions
    # pull from DNAnexus storage with zero ECR dependency.
    for ecr_key, ecr_val in [
        ("ecr_role_arn_to_assume", ecr_role_arn),
        ("ecr_job_token_audience", ecr_job_token_audience),
        ("ecr_job_token_subject_claims", ecr_job_token_subject_claims),
    ]:
        if ecr_val:
            input_hash[ecr_key] = ecr_val

    # Auto-detect NPI capability for version selection
    if nextflow_version is not None:
        # Validate early so invalid versions fail before launching a job; result intentionally discarded.
        # warn=False: deprecation warning will be emitted by the worker's dxpy during the actual build.
        resolve_version(nextflow_version, warn=False)
        # TODO: Remove auto-detect once all deployed NPI versions support nextflow_version input
        if _npi_supports_version_selection():
            input_hash["nextflow_version"] = nextflow_version
        else:
            sys.stderr.write(
                "WARNING: The deployed Nextflow Pipeline Importer does not support "
                "version selection. Building with NPI's default version.\n"
            )

    if destination:
        build_project_id, build_folder, _ = try_call(resolve_existing_path, destination, expected='folder')
    if build_project_id is None:
        parser.error(
            "Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")
    importer = get_importer_object()
    # DXApp.run() uses app_input=; DXApplet.run() uses applet_input= (positional).
    # Branch so both the production DXApp path and the test-override DXApplet path work.
    run_input_kwarg = "applet_input" if isinstance(importer, dxpy.DXApplet) else "app_input"
    nf_builder_job = importer.run(**{run_input_kwarg: input_hash}, project=build_project_id,
                                  folder=build_folder,
                                  name="Nextflow build of %s" % (repository), detach=True)

    if not brief:
        print("Started builder job %s" % (nf_builder_job.get_id(),))
    nf_builder_job.wait_on_done(interval=1)
    applet_id, _ = dxpy.get_dxlink_ids(nf_builder_job.describe(fields={"output": True})['output']['output_applet'])
    if not brief:
        print("Created Nextflow pipeline %s" % (applet_id))
    else:
        print(json.dumps(dict(id=applet_id)))
    return applet_id


def prepare_nextflow(
        resources_dir,
        profile,
        region,
        cache_docker=False,
        nextflow_pipeline_params="",
        nextflow_version=None
):
    """
    :param resources_dir: Directory with all resources needed for the Nextflow pipeline. Usually directory with user's Nextflow files.
    :type resources_dir: str or Path
    :param profile: Custom Nextflow profile. More profiles can be provided by using comma separated string (without whitespaces).
    :type profile: str
    :param region: The region in which the applet will be built.
    :type region: str
    :param cache_docker: Perform pipeline analysis and cache the detected docker images on the platform
    :type cache_docker: boolean
    :param nextflow_pipeline_params: Custom Nextflow pipeline parameters
    :type nextflow_pipeline_params: string
    :returns: Path to the created dxapp_dir
    :rtype: Path

    Creates files necessary for creating an applet on the Platform, such as dxapp.json and a source file. These files are created in '.dx.nextflow' directory.
    """
    assert os.path.exists(resources_dir)
    if not glob(os.path.join(resources_dir, "*.nf")):
        raise dxpy.app_builder.AppBuilderException(
            "Directory %s does not contain Nextflow file (*.nf): not a valid Nextflow directory" % resources_dir)
    dxapp_dir = tempfile.mkdtemp(prefix=".dx.nextflow")

    custom_inputs = prepare_custom_inputs(schema_file=os.path.join(resources_dir, "nextflow_schema.json"))
    dxapp_content = get_nextflow_dxapp(
        custom_inputs=custom_inputs,
        resources_dir=resources_dir,
        region=region,
        profile=profile,
        cache_docker=cache_docker,
        nextflow_pipeline_params=nextflow_pipeline_params,
        nextflow_version=nextflow_version
    )
    exec_content = get_nextflow_src(custom_inputs=custom_inputs, profile=profile, resources_dir=resources_dir)
    shutil.copytree(get_template_dir(), dxapp_dir, dirs_exist_ok=True)
    write_dxapp(dxapp_dir, dxapp_content)
    write_exec(dxapp_dir, exec_content)
    create_readme(resources_dir, dxapp_dir)
    return dxapp_dir


def prepare_custom_inputs(schema_file="./nextflow_schema.json"):
    """
    :param schema_file: path to nextflow_schema.json file
    :type schema_file: str or Path
    :returns: list of custom inputs defined with DNAnexus datatype 
    :rtype: list
    Creates custom input list from nextflow_schema.json that
    will be added in dxapp.json inputSpec field
    """

    def get_dx_type(nf_type, nf_format=None):
        types = {
            "string": "string",
            "integer": "int",
            "number": "float",
            "boolean": "boolean",
            "object": "hash"
        }
        str_types = {
            "file-path": "file",
            "directory-path": "string",  # So far we will stick with strings dx://...
            "path": "string"
        }
        # Handle JSON Schema union types e.g. ["string", "null"], ["boolean", "string"]
        if isinstance(nf_type, list):
            non_null = [t for t in nf_type if t != "null"]
            nf_type = non_null[0] if non_null else "string"
        if nf_type == "string" and nf_format in str_types:
            return str_types[nf_format]
        elif nf_type in types:
            return types[nf_type]
        raise Exception("type {} is not supported by DNAnexus".format(nf_type))

    inputs = []
    if not os.path.exists(schema_file):
        return inputs

    with open(schema_file, "r") as fh:
        schema = json.load(fh)
    defs_key = "definitions" if "definitions" in schema else "$defs" if "$defs" in schema else None
    if defs_key is None:
        return inputs
    for d_key, d_schema in schema.get(defs_key, {}).items():
        required_inputs = d_schema.get("required", [])
        for property_key, property in d_schema.get("properties", {}).items():
            dx_input = {}
            dx_input["name"] = property_key
            dx_input["title"] = dx_input['name']
            dx_input["class"] = get_dx_type(property.get("type"), property.get("format"))

            help_parts = [f"(Nextflow pipeline {'required' if property_key in required_inputs else 'optional'})"]
            if "default" in property:
                help_parts.append(f"Default value: {property.get('default', '')}.")
            help_parts.append(property.get('description', None))
            help_parts.append(property.get('help_text', None))
            dx_input["help"] = " ".join(filter(lambda x: x, help_parts)).strip()

            dx_input["hidden"] = property.get('hidden', False)
            dx_input["optional"] = True
            if property_key in required_inputs:
                inputs.insert(0, dx_input)
            else:
                inputs.append(dx_input)

    return inputs
