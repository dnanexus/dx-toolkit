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
    required_keys = {"status", "nextflow_assets", "nextaur_assets", "awscli_assets", "cache_digest_type"}
    valid_digest_types = {"config", "manifest"}
    for ver, cfg in manifest["versions"].items():
        missing = required_keys - set(cfg.keys())
        if missing:
            raise dxpy.exceptions.DXCLIError(
                f"Malformed version entry '{ver}': missing keys {sorted(missing)}")
        digest_type = cfg.get("cache_digest_type")
        if digest_type not in valid_digest_types:
            raise dxpy.exceptions.DXCLIError(
                f"Invalid cache_digest_type '{digest_type}' in version '{ver}'; expected one of {sorted(valid_digest_types)}")
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
        use_manifest_digest = version_config.get("cache_digest_type") == "manifest" if version_config else False
        image_refs = collect_docker_images(resources_dir, profile, nextflow_pipeline_params, use_manifest_digest=use_manifest_digest)
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


# Keys forwarded from the local nextflow.config to the NPI app at build time so that
# the importer job can mint a JIT, assume the ECR role, and `docker login` to the
# private ECR registry before pulling images via `--cache-docker`.
# Format: (config-key, NPI-input-name). Config keys are looked up in both dotted
# (`dnanexus.iamRoleArnToAssume = ...`) and scope-block (`dnanexus { iamRoleArnToAssume = ... }`)
# form. String values only — these are all string-typed in the runtime config.
_NEXTFLOW_DX_CONFIG_KEYS = [
    ("dnanexus.iamRoleArnToAssume", "iam_role_arn_to_assume"),
    ("dnanexus.jobTokenAudience", "job_token_audience"),
    ("dnanexus.jobTokenSubjectClaims", "job_token_subject_claims"),
    ("dnanexus.ecrRoleArnToAssume", "ecr_role_arn_to_assume"),
    ("dnanexus.ecrJobTokenAudience", "ecr_job_token_audience"),
    ("dnanexus.ecrJobTokenSubjectClaims", "ecr_job_token_subject_claims"),
    ("aws.region", "aws_region"),
]


def _strip_groovy_comments(text):
    """Strip line (`//`) and block (`/* ... */`) comments while preserving the
    contents of string literals.

    Earlier versions stripped `//` unconditionally, which corrupted legal config
    values that happen to contain `//` inside a quoted string (e.g. URI-shaped
    subject claims like `'job://...'` or S3 URLs). We now mask out single- and
    double-quoted string spans before stripping comments, then restore them.

    Newlines inside block comments are preserved (replaced with spaces of the
    same length) so multi-line regexes anchored on `^...$` don't shift line
    boundaries.

    Limitations:
      - Single-line single- or double-quoted strings only. Groovy/Java
        triple-quoted strings (`'''...'''`, `\"\"\"...\"\"\"`) and
        slashy-strings (`/.../`) are NOT recognised; if a config value
        uses one of those forms, comment-stripping may corrupt it. The
        keys consumed by `parse_nextflow_config_dx_fields` (role ARNs,
        OIDC audiences, region names, subject claims) are short tokens
        that fit comfortably on a single line, so this restriction has
        not been observed in practice. The `_accept` helper rejects any
        value containing `\\r` or `\\n` as a defense-in-depth backstop
        (see APPS-3915 BUG-2).
      - `includeConfig` / nested-profile chains are not followed.
    """
    # 1. Mask string literals so their contents are protected from comment
    #    stripping. Each literal becomes \x00<idx>\x00 — a unique slot so two
    #    adjacent literals don't merge during unmasking.
    masked_strings = []

    def _mask(m):
        idx = len(masked_strings)
        masked_strings.append(m.group(0))
        return f"\x00{idx}\x00"

    # Match `"..."` or `'...'` lazily — single-line. Does not handle backslash
    # escapes (the keys we extract are simple ARNs/URIs without escapes).
    text = re.sub(r"\"[^\"\n]*\"|\'[^\'\n]*\'", _mask, text)

    # 2. Strip block comments while preserving line breaks (so line-anchored
    #    regexes elsewhere don't shift).
    def _blank_keep_newlines(m):
        return re.sub(r"[^\n]", " ", m.group(0))

    text = re.sub(r"/\*.*?\*/", _blank_keep_newlines, text, flags=re.DOTALL)

    # 3. Strip line comments.
    text = re.sub(r"//[^\n]*", "", text)

    # 4. Restore masked string literals from their slot indices.
    text = re.sub(r"\x00(\d+)\x00", lambda m: masked_strings[int(m.group(1))], text)
    return text


def parse_nextflow_config_dx_fields(src_dir):
    """Best-effort parser for the subset of `nextflow.config` keys we forward to NPI.

    Supports two layout styles per key:
      1. Dotted:   `dnanexus.iamRoleArnToAssume = 'arn:...'`
      2. Scope:    `dnanexus { iamRoleArnToAssume = 'arn:...' }` (one level deep)

    Limitations (intentional — full Groovy/HOCON parsing is out of scope):
      - Only single-quoted or double-quoted string values are recognized.
      - `includeConfig` / nested profiles are not followed.
      - Variable interpolation, env reads, and groovy expressions are skipped.

    Returns a dict mapping NPI input names (per `_NEXTFLOW_DX_CONFIG_KEYS`) to
    values. Missing keys are simply absent. Returns `{}` if the file does not
    exist or cannot be read.
    """
    if not src_dir:
        return {}
    config_path = path.join(src_dir, "nextflow.config")
    if not path.isfile(config_path):
        return {}
    try:
        with open(config_path, "r", encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except (OSError, IOError):
        return {}
    text = _strip_groovy_comments(text)
    found = {}

    def _accept(value):
        """Reject values that contain a CR or LF.

        The capturing classes `[^']*` / `[^"]*` are negated character classes
        that DO match newlines (the `(?m)` flag only affects `^` / `$`, not
        character classes). A user with control over `nextflow.config` could
        otherwise embed newlines in a value and inject INI sections into the
        importer's `~/.aws/credentials` heredoc on the build path, or smuggle
        extra arguments into commands consuming these values. None of the
        keys we extract (ARNs, audiences, claim names, regions) legitimately
        contain newlines, so reject them outright at the parser boundary —
        this is the same discipline applied to the runtime path by
        AwsUtils.shellSingleQuote which strips \r\n before writing
        /.dx-aws.env.
        """
        if value is None or "\r" in value or "\n" in value:
            return None
        return value

    # --- 1. Dotted-form pass: `scope.key = 'value'` on a single line.
    for cfg_key, npi_name in _NEXTFLOW_DX_CONFIG_KEYS:
        # Match start-of-line whitespace, the literal key, optional whitespace, `=`, then a quoted value.
        pat = re.compile(
            r"(?m)^\s*" + re.escape(cfg_key) + r"\s*=\s*(?:'([^']*)'|\"([^\"]*)\")\s*$"
        )
        m = pat.search(text)
        if m:
            raw = m.group(1) if m.group(1) is not None else m.group(2)
            cleaned = _accept(raw)
            if cleaned is not None:
                found[npi_name] = cleaned

    # --- 2. Scope-block pass: extract `scope { ... }` body, then look for inner keys.
    # Group config keys by their leading scope.
    scope_to_keys = {}
    for cfg_key, npi_name in _NEXTFLOW_DX_CONFIG_KEYS:
        scope, _, leaf = cfg_key.partition(".")
        scope_to_keys.setdefault(scope, []).append((leaf, npi_name))

    for scope, key_pairs in scope_to_keys.items():
        # Find `scope { ... }` blocks. Use a simple brace counter to locate the matching `}`.
        for header in re.finditer(r"(?m)^\s*" + re.escape(scope) + r"\s*\{", text):
            start = header.end()
            depth = 1
            i = start
            while i < len(text) and depth > 0:
                c = text[i]
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                i += 1
            if depth != 0:
                continue  # unbalanced — skip
            body = text[start:i - 1]
            for leaf, npi_name in key_pairs:
                if npi_name in found:
                    continue  # dotted form already won
                # Match `<leaf> = '...'` anywhere in the body, on its own line.
                inner = re.search(
                    r"(?m)^\s*" + re.escape(leaf) + r"\s*=\s*(?:'([^']*)'|\"([^\"]*)\")\s*$",
                    body,
                )
                if inner:
                    raw = inner.group(1) if inner.group(1) is not None else inner.group(2)
                    cleaned = _accept(raw)
                    if cleaned is not None:
                        found[npi_name] = cleaned
    return found


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

