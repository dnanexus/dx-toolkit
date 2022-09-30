#!/usr/bin/env python
#
# Copyright (C) 2013-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.


from __future__ import print_function, unicode_literals, division, absolute_import

import logging
logging.basicConfig(level=logging.WARNING)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)

import os, sys, json, subprocess, argparse
import platform
import py_compile
import re
import shutil
import tempfile
import time
from datetime import datetime
import dxpy
import dxpy.app_builder
import dxpy.workflow_builder
import dxpy.executable_builder
from .. import logger

from dxpy.nextflow.nextflow_builder import build_pipeline_from_repository, prepare_nextflow
from dxpy.nextflow.nextflow_utils import get_resources_subpath

from ..utils import json_load_raise_on_duplicates
from ..utils.resolver import resolve_path, check_folder_exists, ResolutionError, is_container_id
from ..utils.completer import LocalCompleter
from ..app_categories import APP_CATEGORIES
from ..exceptions import err_exit
from ..utils.printing import BOLD
from ..compat import open, USING_PYTHON2, decode_command_line_args, basestring

decode_command_line_args()

parser = argparse.ArgumentParser(description="Uploads a DNAnexus App.")

class DXSyntaxError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

def _get_timestamp_version_suffix(version):
    if "+" in version:
        return ".build." + datetime.today().strftime('%Y%m%d.%H%M')
    else:
        return "+build." + datetime.today().strftime('%Y%m%d.%H%M')

def _get_version_suffix(src_dir, version):
    # If anything goes wrong, fall back to the date-based suffix.
    try:
        if os.path.exists(os.path.join(src_dir, ".git")):
            abbrev_sha1 = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=src_dir).strip()[:7]
            # We ensure that if VERSION is semver-compliant, then
            # VERSION + SUFFIX will be too. In particular that means
            # (here and in _get_timestamp_version_suffix above) we add
            # what semver refers to as a "build metadata" section
            # (delimited by "+"), unless one already exists, in which
            # case we append to the existing one.
            if "+" in version:
                return ".git." + abbrev_sha1
            else:
                return "+git." + abbrev_sha1
    except:
        pass
    return _get_timestamp_version_suffix(version)

def parse_destination(dest_str):
    return dxpy.executable_builder.get_parsed_destination(dest_str)

def _check_suggestions(app_json, publish=False):
    """
    Examines the specified dxapp.json file and warns about any
    violations of suggestions guidelines.

    :raises: AppBuilderException for data objects that could not be found
    """
    for input_field in app_json.get('inputSpec', []):
        for suggestion in input_field.get('suggestions', []):
            if 'project' in suggestion:
                try:
                    project = dxpy.api.project_describe(suggestion['project'], {"permissions": True})
                    if 'PUBLIC' not in project['permissions'] and publish:
                        logger.warn('Project {name} NOT PUBLIC!'.format(name=project['name']))
                except dxpy.exceptions.DXAPIError as e:
                    if e.code == 404:
                        logger.warn('Suggested project {name} does not exist, or not accessible by user'.format(
                                     name=suggestion['project']))
                if 'path' in suggestion:
                    try:
                        check_folder_exists(suggestion['project'], suggestion['path'], '')
                    except ResolutionError as e:
                        logger.warn('Folder {path} could not be found in project {project}'.format(
                                     path=suggestion['path'], project=suggestion['project']))
            if '$dnanexus_link' in suggestion:
                if suggestion['$dnanexus_link'].startswith(('file-', 'record-')):
                    try:
                        dnanexus_link = dxpy.describe(suggestion['$dnanexus_link'])
                    except dxpy.exceptions.DXAPIError as e:
                        if e.code == 404:
                            raise dxpy.app_builder.AppBuilderException(
                                'Suggested object {name} could not be found'.format(
                                    name=suggestion['$dnanexus_link']))
                    except Exception as e:
                        raise dxpy.app_builder.AppBuilderException(str(e))
            if 'value' in suggestion and isinstance(suggestion["value"], dict):
                if '$dnanexus_link' in suggestion['value']:
                    # Check if we have JSON or string
                    if isinstance(suggestion['value']['$dnanexus_link'], dict):
                        if 'project' in suggestion['value']['$dnanexus_link']:
                            try:
                                dxpy.api.project_describe(suggestion['value']['$dnanexus_link']['project'])
                            except dxpy.exceptions.DXAPIError as e:
                                if e.code == 404:
                                    logger.warn('Suggested project {name} does not exist, or not accessible by user'.format(
                                                 name=suggestion['value']['$dnanexus_link']['project']))
                    elif isinstance(suggestion['value']['$dnanexus_link'], basestring):
                        if suggestion['value']['$dnanexus_link'].startswith(('file-', 'record-')):
                            try:
                                dnanexus_link = dxpy.describe(suggestion['value']['$dnanexus_link'])
                            except dxpy.exceptions.DXAPIError as e:
                                if e.code == 404:
                                    raise dxpy.app_builder.AppBuilderException(
                                        'Suggested object {name} could not be found'.format(
                                            name=suggestion['value']['$dnanexus_link']))
                            except Exception as e:
                                raise dxpy.app_builder.AppBuilderException(str(e))

def _lint(dxapp_json_filename, mode):
    """
    Examines the specified dxapp.json file and warns about any
    violations of app guidelines.

    Precondition: the dxapp.json file exists and can be parsed.
    """

    def _find_readme(dirname):
        for basename in ['README.md', 'Readme.md', 'readme.md']:
            if os.path.exists(os.path.join(dirname, basename)):
                return os.path.join(dirname, basename)
        return None

    app_spec = json.load(open(dxapp_json_filename))

    dirname = os.path.basename(os.path.dirname(os.path.abspath(dxapp_json_filename)))

    if mode == "app":
        if 'title' not in app_spec:
            logger.warn('app is missing a title, please add one in the "title" field of dxapp.json')

        if 'summary' in app_spec:
            if app_spec['summary'].endswith('.'):
                logger.warn('summary "%s" should be a short phrase not ending in a period' % (app_spec['summary'],))
        else:
            logger.warn('app is missing a summary, please add one in the "summary" field of dxapp.json')

        readme_filename = _find_readme(os.path.dirname(dxapp_json_filename))
        if 'description' in app_spec:
            if readme_filename:
                raise dxpy.app_builder.AppBuilderException('Description was provided both in Readme.md '
                          'and in the "description" field of {file}. Please consolidate content in Readme.md '
                          'and remove the "description" field.'.format(file=dxapp_json_filename))
            if not app_spec['description'].strip().endswith('.'):
                logger.warn('"description" field should be written in complete sentences and end with a period')
        else:
            if readme_filename is None:
                logger.warn("app is missing a description, please supply one in README.md")
        if 'categories' in app_spec:
            for category in app_spec['categories']:
                if category not in APP_CATEGORIES:
                    logger.warn('app has unrecognized category "%s"' % (category,))
                if category == 'Import':
                    if 'title' in app_spec and not app_spec['title'].endswith('Importer'):
                        logger.warn('title "%s" should end in "Importer"' % (app_spec['title'],))
                if category == 'Export':
                    if 'title' in app_spec and not app_spec['title'].endswith('Exporter'):
                        logger.warn('title "%s" should end in "Exporter"' % (app_spec['title'],))

    if 'name' in app_spec:
        if app_spec['name'] != app_spec['name'].lower():
            logger.warn('name "%s" should be all lowercase' % (app_spec['name'],))
        if dirname != app_spec['name'] and not os.path.abspath(dxapp_json_filename).startswith("/tmp") and not dirname.startswith("."):
            logger.warn('app name "%s" does not match containing directory "%s"' % (app_spec['name'], dirname))
    else:
        logger.warn('app is missing a name, please add one in the "name" field of dxapp.json')

    if 'version' in app_spec:
        if not dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match(app_spec['version']):
            logger.warn('"version" %s should be semver compliant (e.g. of the form X.Y.Z)' % (app_spec['version'],))

    # Note that identical checks are performed on the server side (and
    # will cause the app build to fail), but the checks here are printed
    # sooner and multiple naming problems can be detected in a single
    # pass.
    if 'inputSpec' in app_spec:
        for i, input_field in enumerate(app_spec['inputSpec']):
            if not re.match("^[a-zA-Z_][0-9a-zA-Z_]*$", input_field['name']):
                logger.error('input %d has illegal name "%s" (must match ^[a-zA-Z_][0-9a-zA-Z_]*$)' % (i, input_field['name']))
    else:
        logger.warn("dxapp.json contains no input specification (inputSpec). Your applet will not be usable as an " +
                    "app, runnable from the GUI, or composable using workflows.")
    if 'outputSpec' in app_spec:
        for i, output_field in enumerate(app_spec['outputSpec']):
            if not re.match("^[a-zA-Z_][0-9a-zA-Z_]*$", output_field['name']):
                logger.error('output %d has illegal name "%s" (must match ^[a-zA-Z_][0-9a-zA-Z_]*$)' % (i, output_field['name']))
    else:
        logger.warn("dxapp.json contains no output specification (outputSpec). Your applet will not be usable as an " +
                    "app, runnable from the GUI, or composable using workflows.")

def _check_syntax(code, lang, temp_dir, enforce=True):
    """
    Checks that the code whose text is in CODE parses as LANG.

    Raises DXSyntaxError if there is a problem and "enforce" is True.
    """
    # This function needs the language to be explicitly set, so we can
    # generate an appropriate temp filename.
    if lang == 'python2.7' or 'python3' in lang:
        temp_basename = 'inlined_code_from_dxapp_json.py'
    elif lang == 'bash':
        temp_basename = 'inlined_code_from_dxapp_json.sh'
    else:
        raise ValueError('lang must be one of "python2.7", "python3", or "bash"')
    # Dump the contents out to a temporary file, then call _check_file_syntax.
    with open(os.path.join(temp_dir, temp_basename), 'w') as ofile:
        ofile.write(code)
    _check_file_syntax(os.path.join(temp_dir, temp_basename), temp_dir, override_lang=lang, enforce=enforce)


# convert the error message embedded in the exception to a proper string
def _error_message_to_string(e, message):
    if isinstance(message, str):
        return message
    elif isinstance(message, bytes):
        return message.decode("utf-8")
    else:
        # What kind of object is this?
        print("The error message is neither string nor bytes, it is {}".format(type(message)))
        raise e

def _check_file_syntax(filename, temp_dir, override_lang=None, enforce=True):
    """
    Checks that the code in FILENAME parses, attempting to autodetect
    the language if necessary.

    Raises IOError if the file cannot be read.

    Raises DXSyntaxError if there is a problem and "enforce" is True.
    """
    def check_python(filename):
        # Generate a semi-recognizable name to write the pyc to. Of
        # course it's possible that different files being scanned could
        # have the same basename, so this path won't be unique, but the
        # checks don't run concurrently so this shouldn't cause any
        # problems.
        pyc_path = os.path.join(temp_dir, os.path.basename(filename) + ".pyc")
        try:
            if USING_PYTHON2:
                filename = filename.encode(sys.getfilesystemencoding())
            py_compile.compile(filename, cfile=pyc_path, doraise=True)
        finally:
            try:
                os.unlink(pyc_path)
            except OSError:
                pass
    def check_bash(filename):
        if platform.system() == 'Windows':
            logging.warn(
                    'Skipping bash syntax check due to unavailability of bash on Windows.')
        else:
            subprocess.check_output(["/bin/bash", "-n", filename], stderr=subprocess.STDOUT)
    python_unsure = False
    if override_lang == 'python2.7' and USING_PYTHON2:
        checker_fn = check_python
    elif override_lang == 'python3' and not USING_PYTHON2:
        checker_fn = check_python
    elif override_lang == 'bash':
        checker_fn = check_bash
    elif filename.endswith('.py'):
        checker_fn = check_python
        # don't enforce and ignore if the shebang is ambiguous and we're not sure
        # that the file version is the same as the one we're running
        read_mode = "r"
        if USING_PYTHON2:
            read_mode = "rb"
        with open(filename, read_mode) as f:
            first_line = f.readline()
            if not (('python3' in first_line and not USING_PYTHON2) or
                    ('python2' in first_line and USING_PYTHON2)):
                enforce = False
                python_unsure = True
    elif filename.endswith('.sh'):
        checker_fn = check_bash
    else:
        # Ignore other kinds of files.
        return

    # Do a test read of the file to catch errors like the file not
    # existing or not being readable.
    open(filename)

    try:
        checker_fn(filename)
    except subprocess.CalledProcessError as e:
        print(filename + " has a syntax error! Interpreter output:", file=sys.stderr)
        if USING_PYTHON2:
            errmsg = e.output
        else:
            errmsg = _error_message_to_string(e, e.output)
        for line in errmsg.strip("\n").split("\n"):
            print("  " + line.rstrip("\n"), file=sys.stderr)
        if enforce:
            raise DXSyntaxError(filename + " has a syntax error")
    except py_compile.PyCompileError as e:
        if python_unsure:
            print("Unsure if " + filename + " is using Python 2 or Python 3, the following error might not be relevant", file=sys.stderr)
        print(filename + " has a syntax error! Interpreter output:", file=sys.stderr)
        if USING_PYTHON2:
            errmsg = e.msg
        else:
            errmsg = _error_message_to_string(e, e.msg)
        print("  " + errmsg.strip(), file=sys.stderr)
        if enforce:
            raise DXSyntaxError(e.msg.strip())


def _verify_app_source_dir_impl(src_dir, temp_dir, mode, enforce=True):
    """Performs syntax and lint checks on the app source.

    Precondition: the dxapp.json file exists and can be parsed.
    """
    _lint(os.path.join(src_dir, "dxapp.json"), mode)

    # Check that the entry point file parses as the type it is going to
    # be interpreted as. The extension is irrelevant.
    manifest = json.load(open(os.path.join(src_dir, "dxapp.json")))
    if "runSpec" in manifest:
        if "interpreter" not in manifest['runSpec']:
            raise dxpy.app_builder.AppBuilderException('runSpec.interpreter field was not present')

        if "release" not in manifest['runSpec'] or "distribution" not in manifest['runSpec']:
            warn_message = 'runSpec.distribution or runSpec.release was not present. These fields '
            warn_message += 'will be required in a future version of the API. Recommended value '
            warn_message += 'for distribution is \"Ubuntu\" and release - \"14.04\".'
            logger.warn(warn_message)

        if manifest['runSpec']['interpreter'] in ["python2.7", "bash", "python3"]:
            if "file" in manifest['runSpec']:
                entry_point_file = os.path.abspath(os.path.join(src_dir, manifest['runSpec']['file']))
                try:
                    _check_file_syntax(entry_point_file, temp_dir, override_lang=manifest['runSpec']['interpreter'], enforce=enforce)
                except IOError as e:
                    raise dxpy.app_builder.AppBuilderException(
                        'Could not open runSpec.file=%r. The problem was: %s' % (entry_point_file, e))
                except DXSyntaxError:
                    raise dxpy.app_builder.AppBuilderException('Entry point file %s has syntax errors, see above for details. Rerun with --no-check-syntax to proceed anyway.' % (entry_point_file,))
            elif "code" in manifest['runSpec']:
                try:
                    _check_syntax(manifest['runSpec']['code'], manifest['runSpec']['interpreter'], temp_dir, enforce=enforce)
                except DXSyntaxError:
                    raise dxpy.app_builder.AppBuilderException('Code in runSpec.code has syntax errors, see above for details. Rerun with --no-check-syntax to proceed anyway.')

            cluster_bootstrap_scripts = _get_all_cluster_bootstrap_script_names(manifest)
            for filename in cluster_bootstrap_scripts:
                abs_filename = os.path.abspath(os.path.join(src_dir, filename))
                try:
                    _check_file_syntax(abs_filename,
                                       temp_dir,
                                       override_lang=manifest['runSpec']['interpreter'],
                                       enforce=enforce)
                except IOError as e:
                    raise dxpy.app_builder.AppBuilderException(
                        'Could not open cluster bootstrap script %r. The problem was: %s' % (abs_filename, e))
                except DXSyntaxError:
                    raise dxpy.app_builder.AppBuilderException('Code in cluster bootstrapScript %r has syntax errors, see above for details. Rerun with --no-check-syntax to proceed anyway.' % filename)

        if 'execDepends' in manifest['runSpec']:
            if not isinstance(manifest['runSpec']['execDepends'], list):
                raise dxpy.app_builder.AppBuilderException('Expected runSpec.execDepends to be an array. Rerun with --no-check-syntax to proceed anyway.')
            if not all(isinstance(dep, dict) for dep in manifest['runSpec']['execDepends']):
                raise dxpy.app_builder.AppBuilderException('Expected runSpec.execDepends to be an array of hashes. Rerun with --no-check-syntax to proceed anyway.')
            if any(dep.get('package_manager', 'apt') != 'apt' for dep in manifest['runSpec']['execDepends']):
                if not isinstance(manifest.get('access'), dict) or 'network' not in manifest['access']:
                    msg = '\n'.join(['runSpec.execDepends specifies non-APT dependencies, but no network access spec is given.',
                    'Add {"access": {"network": ["*"]}} to allow dependencies to install.',
                    'See https://documentation.dnanexus.com/developer/apps/execution-environment#network-access.',
                    'Rerun with --no-check-syntax to proceed anyway.'])
                    raise dxpy.app_builder.AppBuilderException(msg)

    if 'authorizedUsers' in manifest:
        if not isinstance(manifest['authorizedUsers'], list) or isinstance(manifest['authorizedUsers'], basestring):
            raise dxpy.app_builder.AppBuilderException('Expected authorizedUsers to be a list of strings')
        for thing in manifest['authorizedUsers']:
            if thing != 'PUBLIC' and (not isinstance(thing, basestring) or not re.match("^(org-|user-)", thing)):
                raise dxpy.app_builder.AppBuilderException('authorizedUsers field contains an entry which is not either the string "PUBLIC" or a user or org ID')

    if "pricingPolicy" in manifest:
        error_message = "\"pricingPolicy\" at the top level is not accepted. It must be specified "
        error_message += "under the \"regionalOptions\" field in all enabled regions of the app"
        raise dxpy.app_builder.AppBuilderException(error_message)

    # Check all other files that are going to be in the resources tree.
    # For these we detect the language based on the filename extension.
    # Obviously this check can have false positives, since the app can
    # execute (or not execute!) all these files in whatever way it
    # wishes, e.g. it could use Python != 2.7 or some non-bash shell.
    # Consequently errors here are non-fatal.
    files_with_problems = []
    for dirpath, dirnames, filenames in os.walk(os.path.abspath(os.path.join(src_dir, "resources"))):
        for filename in filenames:
            # On Mac OS, the resource fork for "FILE.EXT" gets tarred up
            # as a file named "._FILE.EXT". To a naive check this
            # appears to be a file of the same extension. Therefore, we
            # exclude these from syntax checking since they are likely
            # to not parse as whatever language they appear to be.
            if not filename.startswith("._"):
                try:
                    _check_file_syntax(os.path.join(dirpath, filename), temp_dir, enforce=True)
                except IOError as e:
                    raise dxpy.app_builder.AppBuilderException(
                        'Could not open file in resources directory %r. The problem was: %s' %
                        (os.path.join(dirpath, filename), e)
                    )
                except DXSyntaxError:
                    # Suppresses errors from _check_file_syntax so we
                    # only print a nice error message
                    files_with_problems.append(os.path.join(dirpath, filename))

    if files_with_problems:
        # Make a message of the form:
        #    "/path/to/my/app.py"
        # OR "/path/to/my/app.py and 3 other files"
        files_str = files_with_problems[0] if len(files_with_problems) == 1 else (files_with_problems[0] + " and " + str(len(files_with_problems) - 1) + " other file" + ("s" if len(files_with_problems) > 2 else ""))
        logging.warn('%s contained syntax errors, see above for details' % (files_str,))


def _get_all_cluster_bootstrap_script_names(manifest):
    # runSpec.systemRequirements is deprecated. Enforce use of regionalOptions.
    if 'systemRequirements' in manifest['runSpec']:
        sys_reqs = manifest['runSpec']['systemRequirements']
        for entry_point in sys_reqs:
            if 'clusterSpec' in sys_reqs[entry_point]:
                err_msg = "\"clusterSpec\" in \"runSpec.systemRequirements\" is not accepted."
                err_msg += " It must be specified in \"systemRequirements\" under the \"regionalOptions\" field in all enabled regions of the app."
                raise dxpy.app_builder.AppBuilderException(err_msg)

    script_names = []
    if 'regionalOptions' in manifest:
        for region in manifest['regionalOptions']:
            if 'systemRequirements' in manifest['regionalOptions'][region]:
                sys_reqs = manifest['regionalOptions'][region]['systemRequirements']
                for entry_point in sys_reqs:
                    try:
                        filename = sys_reqs[entry_point]['clusterSpec']['bootstrapScript']
                        script_names.append(filename)
                    except KeyError:
                        # either no "clusterSpec" or no "bootstrapScript" within "clusterSpec"
                        continue

    return script_names

def _verify_app_source_dir(src_dir, mode, enforce=True):
    """Performs syntax and lint checks on the app source.

    Precondition: the dxapp.json file exists and can be parsed.
    """
    temp_dir = tempfile.mkdtemp(prefix='dx-build_tmp')
    try:
        _verify_app_source_dir_impl(src_dir, temp_dir, mode, enforce=enforce)
    finally:
        shutil.rmtree(temp_dir)

def _parse_app_spec(src_dir):
    """Returns the parsed contents of dxapp.json.

    Raises either AppBuilderException or a parser error (exit codes 3 or
    2 respectively) if this cannot be done.
    """
    if not os.path.isdir(src_dir):
        parser.error("%s is not a directory" % src_dir)
    if not os.path.exists(os.path.join(src_dir, "dxapp.json")):
        raise dxpy.app_builder.AppBuilderException("Directory %s does not contain dxapp.json: not a valid DNAnexus app source directory" % src_dir)
    with open(os.path.join(src_dir, "dxapp.json")) as app_desc:
        try:
            return json_load_raise_on_duplicates(app_desc)
        except Exception as e:
            raise dxpy.app_builder.AppBuilderException("Could not parse dxapp.json file as JSON: " + str(e.args))

def _build_app_remote(mode, src_dir, publish=False, destination_override=None,
                      version_override=None, bill_to_override=None,
                      do_version_autonumbering=True, do_try_update=True, do_parallel_build=True,
                      do_check_syntax=True, region=None, watch=True):
    if mode == 'app':
        builder_app = 'app-tarball_app_builder'
    else:
        builder_app = 'app-tarball_applet_builder'

    app_spec = _parse_app_spec(src_dir)
    builder_versions = {"12.04": "", "14.04": "_trusty", "16.04": "_xenial",
                        "20.04": "_focal"}
    release = app_spec['runSpec'].get('release')
    # Remote builder app/applet for 16.04 version 1
    if release == "16.04" and app_spec['runSpec'].get('version', '0') == '1':
        builder_app += "_xenial_v1"
    else:
        builder_app += builder_versions.get(release, "")


    temp_dir = tempfile.mkdtemp()

    build_options = {}

    if version_override:
        build_options['version_override'] = version_override
    elif do_version_autonumbering:
        # If autonumbering is DISABLED, the interior run of dx-build-app
        # will detect the correct version to use without our help. If it
        # is ENABLED, the version suffix might depend on the state of
        # the git repository. Since we'll remove the .git directory
        # before uploading, we need to determine the correct version to
        # use here and pass it in to the interior run of dx-build-app.
        if do_version_autonumbering:
            original_version = app_spec['version']
            app_describe = None
            try:
                app_describe = dxpy.api.app_describe("app-" + app_spec["name"], alias=original_version, always_retry=False)
            except dxpy.exceptions.DXAPIError as e:
                if e.name == 'ResourceNotFound' or (mode == 'applet' and e.name == 'PermissionDenied'):
                    pass
                else:
                    raise e
            if app_describe is not None:
                if 'published' in app_describe or not do_try_update:
                    # The version we wanted was taken; fall back to the
                    # autogenerated version number.
                    build_options['version_override'] = original_version + _get_version_suffix(src_dir, original_version)

    # The following flags are basically passed through verbatim.
    if bill_to_override:
        build_options['bill_to_override'] = bill_to_override
    if not do_version_autonumbering:
        build_options['do_version_autonumbering'] = False
    if not do_try_update:
        build_options['do_try_update'] = False
    if not do_parallel_build:
        build_options['do_parallel_build'] = False
    if not do_check_syntax:
        build_options['do_check_syntax'] = False

    using_temp_project_for_remote_build = False

    # If building an applet, run the builder app in the destination
    # project. If building an app, run the builder app in a temporary
    # project.
    dest_folder = None
    dest_applet_name = None
    if mode == "applet":
        # Translate the --destination flag as follows. If --destination
        # is PROJ:FOLDER/NAME,
        #
        # 1. Run the builder app in PROJ
        # 2. Make the output folder FOLDER
        # 3. Supply --destination=NAME to the interior call of dx-build-applet.
        build_project_id = dxpy.WORKSPACE_ID
        if destination_override:
            build_project_id, dest_folder, dest_applet_name = parse_destination(destination_override)
        if build_project_id is None:
            parser.error("Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")
        if dest_applet_name:
            build_options['destination_override'] = '/' + dest_applet_name

    elif mode == "app":
        using_temp_project_for_remote_build = True
        try:
            project_input = {}
            project_input["name"] = "dx-build-app --remote temporary project"
            if bill_to_override:
                project_input["billTo"] = bill_to_override
            if region:
                project_input["region"] = region
            build_project_id = dxpy.api.project_new(project_input)["id"]
        except:
            err_exit()

    try:
        # Resolve relative paths and symlinks here so we have something
        # reasonable to write in the job name below.
        src_dir = os.path.realpath(src_dir)

        # Show the user some progress as the tarball is being generated.
        # Hopefully this will help them to understand when their tarball
        # is huge (e.g. the target directory already has a whole bunch
        # of binaries in it) and interrupt before uploading begins.
        app_tarball_file = os.path.join(temp_dir, "app_tarball.tar.gz")
        tar_subprocess = subprocess.Popen(["tar", "-czf", "-", "--exclude", "./.git", "."], cwd=src_dir, stdout=subprocess.PIPE)
        with open(app_tarball_file, 'wb') as tar_output_file:
            total_num_bytes = 0
            last_console_update = 0
            start_time = time.time()
            printed_static_message = False
            # Pipe the output of tar into the output file
            while True:
                tar_exitcode = tar_subprocess.poll()
                data = tar_subprocess.stdout.read(4 * 1024 * 1024)
                if tar_exitcode is not None and len(data) == 0:
                    break
                tar_output_file.write(data)
                total_num_bytes += len(data)
                current_time = time.time()
                # Don't show status messages at all for very short tar
                # operations (< 1.0 sec)
                if current_time - last_console_update > 0.25 and current_time - start_time > 1.0:
                    if sys.stderr.isatty():
                        if last_console_update > 0:
                            sys.stderr.write("\r")
                        sys.stderr.write("Compressing target directory {dir}... ({kb_so_far:,} kb)".format(dir=src_dir, kb_so_far=total_num_bytes // 1024))
                        sys.stderr.flush()
                        last_console_update = current_time
                    elif not printed_static_message:
                        # Print a message (once only) when stderr is not
                        # going to a live console
                        sys.stderr.write("Compressing target directory %s..." % (src_dir,))
                        printed_static_message = True

        if last_console_update > 0:
            sys.stderr.write("\n")
        if tar_exitcode != 0:
            raise Exception("tar exited with non-zero exit code " + str(tar_exitcode))

        dxpy.set_workspace_id(build_project_id)

        remote_file = dxpy.upload_local_file(app_tarball_file, media_type="application/gzip",
                                             wait_on_close=True, show_progress=True)

        try:
            input_hash = {
                "input_file": dxpy.dxlink(remote_file),
                "build_options": build_options
                }
            if mode == 'app':
                input_hash["publish"] = publish
            api_options = {
                "name": "Remote build of %s" % (os.path.basename(src_dir),),
                "input": input_hash,
                "project": build_project_id,
                }
            if dest_folder:
                api_options["folder"] = dest_folder
            app_run_result = dxpy.api.app_run(builder_app, input_params=api_options)
            job_id = app_run_result["id"]
            print("Started builder job %s" % (job_id,))
            if watch:
                try:
                    subprocess.check_call(["dx", "watch", job_id])
                except subprocess.CalledProcessError as e:
                    if e.returncode == 3:
                        # Some kind of failure to build the app. The reason
                        # for the failure is probably self-evident from the
                        # job log (and if it's not, the CalledProcessError
                        # is not informative anyway), so just propagate the
                        # return code without additional remarks.
                        sys.exit(3)
                    else:
                        raise e

            dxpy.DXJob(job_id).wait_on_done(interval=1)

            if mode == 'applet':
                applet_id, _ = dxpy.get_dxlink_ids(dxpy.api.job_describe(job_id)['output']['output_applet'])
                return applet_id
            else:
                # TODO: determine and return the app ID, to allow
                # running the app if args.run is specified
                return None
        finally:
            if not using_temp_project_for_remote_build:
                dxpy.DXProject(build_project_id).remove_objects([remote_file.get_id()])
    finally:
        if using_temp_project_for_remote_build:
            dxpy.api.project_destroy(build_project_id, {"terminateJobs": True})
        shutil.rmtree(temp_dir)


def build_app_from(applet_desc, version, publish=False, do_try_update=True, bill_to_override=None,
                   return_object_dump=False, confirm=True, brief=False, **kwargs):

    applet_id = applet_desc["id"]
    app_name = applet_desc["name"]
    dxpy.executable_builder.verify_developer_rights('app-' + app_name)
    if not brief:
        logger.info("Will create app from the applet: %s (%s)" % (applet_desc["name"], applet_desc['id'],))

    applet_region = dxpy.api.project_describe(applet_desc["project"],
                                              input_params={"fields": {"region": True}})["region"]

    #TODO: make it possible to build multi region app by uploadling
    # the applet tarball to different regions
    regional_options = {
        applet_region: {'applet': applet_id}
    }

    # Certain metadata is not copied from an applet to the app
    # It must be passed explicitly otherwise default values will be
    # set by the API server or an error will be throw during app build
    # for required non-empty fields
    fields_to_inherit = (
        "summary", "title", "description", "developerNotes",
        "details", "access", "ignoreReuse"
    )
    inherited_metadata = {}
    for field in fields_to_inherit: 
        if field in applet_desc:
            inherited_metadata[field] = applet_desc[field]

    required_non_empty = ("summary", "title", "description", "developerNotes")
    for field in required_non_empty:
        if field not in inherited_metadata or not inherited_metadata[field]:
            inherited_metadata[field] = applet_desc["name"]
    
    app_id = dxpy.app_builder.create_app_multi_region(regional_options,
                                                      app_name,
                                                      None,
                                                      publish=publish,
                                                      set_default=publish,
                                                      billTo=bill_to_override,
                                                      try_versions=version,
                                                      try_update=do_try_update,
                                                      confirm=confirm,
                                                      inherited_metadata=inherited_metadata,
                                                      brief=brief)
    app_describe = dxpy.api.app_describe(app_id)

    if not brief:
        if publish:
            logger.info("Uploaded and published app %s/%s (%s) successfully" % (app_describe["name"], app_describe["version"], app_id))
        else:
            logger.info("Uploaded app %s/%s (%s) successfully" % (app_describe["name"], app_describe["version"], app_id))
            logger.info("You can publish this app with: dx publish {n}/{v}".format(n=app_describe["name"], v=app_describe["version"]))

    return app_describe if return_object_dump else {"id": app_id}


def build_and_upload_locally(src_dir, mode, overwrite=False, archive=False, publish=False, destination_override=None,
                             version_override=None, bill_to_override=None, use_temp_build_project=True,
                             do_parallel_build=True, do_version_autonumbering=True, do_try_update=True,
                             do_check_syntax=True, dry_run=False,
                             return_object_dump=False, confirm=True, ensure_upload=False, force_symlinks=False,
                             region=None, brief=False, resources_dir=None, worker_resources_subpath="", **kwargs):
    dxpy.app_builder.build(src_dir, parallel_build=do_parallel_build)
    app_json = _parse_app_spec(src_dir)

    _check_suggestions(app_json, publish=publish)
    _verify_app_source_dir(src_dir, mode, enforce=do_check_syntax)
    if mode == "app" and not dry_run:
        dxpy.executable_builder.verify_developer_rights('app-' + app_json['name'])

    working_project = None
    using_temp_project = False
    override_folder = None
    override_applet_name = None

    enabled_regions = dxpy.app_builder.get_enabled_regions(app_json, region)

    # Cannot build multi-region app if `use_temp_build_project` is false.
    if enabled_regions is not None and len(enabled_regions) > 1 and not use_temp_build_project:
        raise dxpy.app_builder.AppBuilderException("Cannot specify --no-temp-build-project when building multi-region apps")

    # Prepare projects in which the app's underlying applets will be built (one per region).
    projects_by_region = None
    if mode == "applet" and destination_override:
        working_project, override_folder, override_applet_name = parse_destination(destination_override)
        region = dxpy.api.project_describe(working_project,
                                           input_params={"fields": {"region": True}})["region"]
        projects_by_region = {region: working_project}
    elif mode == "app" and use_temp_build_project and not dry_run:
        projects_by_region = {}
        if enabled_regions is not None:
            # Create temporary projects in each enabled region.
            try:
                for region in enabled_regions:
                    project_input = {
                        "name": "Temporary build project for dx-build-app in {r}".format(r=region),
                        "region": region
                    }
                    if bill_to_override:
                        project_input["billTo"] = bill_to_override
                    working_project = dxpy.api.project_new(project_input)["id"]
                    projects_by_region[region] = working_project
                    logger.debug("Created temporary project %s to build in" % (working_project,))
            except:
                # A /project/new request may fail if the requesting user is
                # not authorized to create projects in a certain region.
                dxpy.executable_builder.delete_temporary_projects(list(projects_by_region.values()))
                err_exit()
        else:
            # Create a temp project
            try:
                project_input = {"name": "Temporary build project for dx-build-app"}
                if bill_to_override:
                    project_input["billTo"] = bill_to_override
                working_project = dxpy.api.project_new(project_input)["id"]
            except:
                err_exit()
            region = dxpy.api.project_describe(working_project,
                                               input_params={"fields": {"region": True}})["region"]
            projects_by_region[region] = working_project
            logger.debug("Created temporary project %s to build in" % (working_project,))

        using_temp_project = True
    elif mode == "app" and not dry_run:
        # If we are not using temporary project(s) to build the executable,
        # then we should have a project context somewhere.
        try:
            project = app_json.get("project", dxpy.WORKSPACE_ID)
            region = dxpy.api.project_describe(project,
                                               input_params={"fields": {"region": True}})["region"]
        except Exception:
            err_exit()
        projects_by_region = {region: project}

    try:
        if mode == "applet" and working_project is None and dxpy.WORKSPACE_ID is None:
            parser.error("Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")

        if mode == "applet":
            dest_project = working_project or dxpy.WORKSPACE_ID or app_json.get("project", False)
            try:
                region = dxpy.api.project_describe(dest_project,
                                                   input_params={"fields": {"region": True}})["region"]
            except Exception:
                err_exit()
            projects_by_region = {region: dest_project}

            if not overwrite and not archive:
                # If we cannot overwite or archive an existing applet and an
                # applet in the destination exists with the same name as this
                # one, then we should err out *before* uploading resources.
                try:
                    if not override_applet_name and kwargs.get("name"):
                        override_applet_name = kwargs.get("name")
                    dest_name = override_applet_name or app_json.get('name') or os.path.basename(os.path.abspath(src_dir))
                except:
                    raise dxpy.app_builder.AppBuilderException("Could not determine applet name from specification + "
                                                               "(dxapp.json) or from working directory (%r)" % (src_dir,))
                dest_folder = override_folder or app_json.get('folder') or '/'
                if not dest_folder.endswith('/'):
                    dest_folder = dest_folder + '/'
                for result in dxpy.find_data_objects(classname="applet", name=dest_name, folder=dest_folder,
                                                     project=dest_project, recurse=False):
                    dest_path = dest_folder + dest_name
                    msg = "An applet already exists at {} (id {}) and neither".format(dest_path, result["id"])
                    msg += " -f/--overwrite nor -a/--archive were given."
                    raise dxpy.app_builder.AppBuilderException(msg)

        if dry_run:
            # Set a dummy "projects_by_region" so that we can exercise the dry
            # run flows for uploading resources bundles and applets below.
            projects_by_region = {"dummy-cloud:dummy-region": "project-dummy"}

        if projects_by_region is None:
            raise AssertionError("'projects_by_region' should not be None at this point")

        # "resources" can be used only with an app enabled in a single region and when
        # "regionalOptions" field is not specified.
        if "resources" in app_json and ("regionalOptions" in app_json or len(projects_by_region) > 1):
            error_message = "dxapp.json cannot contain a top-level \"resources\" field "
            error_message += "when the \"regionalOptions\" field is used or when "
            error_message += "the app is enabled in multiple regions"
            raise dxpy.app_builder.AppBuilderException(error_message)

        resources_bundles_by_region = {}
        for region, project in list(projects_by_region.items()):
            resources_bundles_by_region[region] = dxpy.app_builder.upload_resources(
                src_dir,
                project=project,
                folder=override_folder,
                ensure_upload=ensure_upload,
                force_symlinks=force_symlinks,
                brief=brief,
                resources_dir=resources_dir,
                worker_resources_subpath=worker_resources_subpath) if not dry_run else []

        # TODO: Clean up these applets if the app build fails.
        applet_ids_by_region = {}
        try:
            for region, project in list(projects_by_region.items()):
                applet_id, applet_spec = dxpy.app_builder.upload_applet(
                    src_dir,
                    resources_bundles_by_region[region],
                    check_name_collisions=(mode == "applet"),
                    overwrite=overwrite and mode == "applet",
                    archive=archive and mode == "applet",
                    project=project,
                    override_folder=override_folder,
                    override_name=override_applet_name,
                    dry_run=dry_run,
                    brief=brief,
                    **kwargs)
                if not dry_run:
                    logger.debug("Created applet " + applet_id + " successfully")
                applet_ids_by_region[region] = applet_id
        except:
            # Avoid leaking any bundled_resources files we may have
            # created, if applet creation fails. Note that if
            # using_temp_project, the entire project gets destroyed at
            # the end, so we don't bother.
            if not using_temp_project:
                for region, project in list(projects_by_region.items()):
                    objects_to_delete = [dxpy.get_dxlink_ids(bundled_resource_obj['id'])[0] for bundled_resource_obj in resources_bundles_by_region[region]]
                    if objects_to_delete:
                        dxpy.api.project_remove_objects(
                            dxpy.app_builder.get_destination_project(src_dir, project=project),
                            input_params={"objects": objects_to_delete})
            raise

        if dry_run:
            return

        applet_name = applet_spec['name']

        if mode == "app":
            if 'version' not in app_json:
                parser.error("dxapp.json contains no \"version\" field, but it is required to build an app")
            version = app_json['version']
            try_versions = [version_override or version]
            if not version_override and do_version_autonumbering:
                try_versions.append(version + _get_version_suffix(src_dir, version))

            additional_resources_by_region = {}
            if "regionalOptions" in app_json:
                for region, region_config in list(app_json["regionalOptions"].items()):
                    if "resources" in region_config:
                        additional_resources_by_region[region] = region_config["resources"]
            elif "resources" in app_json:
                additional_resources_by_region[list(projects_by_region.keys())[0]] = app_json["resources"]

            regional_options = {}
            for region in projects_by_region:
                regional_options[region] = {"applet": applet_ids_by_region[region]}
                if region in additional_resources_by_region:
                    regional_options[region]["resources"] = additional_resources_by_region[region]

            # add pricingPolicy separately for better readability
            if "regionalOptions" in app_json:
                for region, region_config in list(app_json["regionalOptions"].items()):
                    if "pricingPolicy" in region_config:
                        regional_options[region]["pricingPolicy"] = region_config["pricingPolicy"]

            app_id = dxpy.app_builder.create_app_multi_region(regional_options,
                                                              applet_name,
                                                              src_dir,
                                                              publish=publish,
                                                              set_default=publish,
                                                              billTo=bill_to_override,
                                                              try_versions=try_versions,
                                                              try_update=do_try_update,
                                                              confirm=confirm,
                                                              brief=brief)

            app_describe = dxpy.api.app_describe(app_id)

            if not brief:
                if publish:
                    logger.info("Uploaded and published app %s/%s (%s) successfully" % (app_describe["name"], app_describe["version"], app_id))
                else:
                    logger.info("Uploaded app %s/%s (%s) successfully" % (app_describe["name"], app_describe["version"], app_id))
                    logger.info("You can publish this app with: dx publish {n}/{v}".format(n=app_describe["name"], v=app_describe["version"]))

            return app_describe if return_object_dump else {"id": app_id}

        elif mode == "applet":
            return dxpy.api.applet_describe(applet_id) if return_object_dump else {"id": applet_id}
        else:
            raise dxpy.app_builder.AppBuilderException("Unrecognized mode %r" % (mode,))

    finally:
        # Clean up after ourselves.
        if using_temp_project:
            dxpy.executable_builder.delete_temporary_projects(list(projects_by_region.values()))

def get_destination_region(destination):
    """
    :param destination: The destination path for building the applet, as given by the --destination option to "dx build". Will be in the form [PROJECT_NAME_OR_ID:][/[FOLDER/][NAME]].
    :type destination: str

    :returns: The name of the region in which the applet will be built, e.g. 'aws:us-east-1'. It doesn't take into account the destination project specified in dxapp.json.
    :rtype: str
    """
    if destination:
        dest_project_id, _, _ = parse_destination(destination)
    else:
        dest_project_id = dxpy.WORKSPACE_ID
    return dxpy.api.project_describe(dest_project_id, input_params={"fields": {"region": True}})["region"]


def _build_app(args, extra_args):
    """Builds an app or applet and returns the resulting executable ID
    (unless it was a dry-run, in which case None is returned).

    TODO: remote app builds still return None, but we should fix this.

    """
    resources_dir = None
    source_dir = args.src_dir
    worker_resources_subpath = ""  # no subpath, files will be saved to root directory by default.
    if args.nextflow and not args.repository:
        source_dir = prepare_nextflow(args.src_dir, args.profile, get_destination_region(args.destination))
        resources_dir = args.src_dir
        worker_resources_subpath = get_resources_subpath(resources_dir)
    if args._from:
        # BUILD FROM EXISTING APPLET
        output = build_app_from(
            args._from,
            [args.version_override],
            publish=args.publish,
            do_try_update=args.update,
            bill_to_override=args.bill_to,
            confirm=args.confirm,
            return_object_dump=args.json,
            brief=args.brief,
            **extra_args
        )
        if output is not None and args.run is None:
            print(json.dumps(output))

        return output['id']

    if not args.remote and not args.repository:  # building with NF repository is implicitly remote
        # LOCAL BUILD
        output = build_and_upload_locally(
            source_dir,
            args.mode,
            overwrite=args.overwrite,
            archive=args.archive,
            publish=args.publish,
            destination_override=args.destination,
            version_override=args.version_override,
            bill_to_override=args.bill_to,
            use_temp_build_project=args.use_temp_build_project,
            do_parallel_build=args.parallel_build,
            do_version_autonumbering=args.version_autonumbering,
            do_try_update=args.update,
            do_check_syntax=args.check_syntax,
            ensure_upload=args.ensure_upload,
            force_symlinks=args.force_symlinks,
            dry_run=args.dry_run,
            confirm=args.confirm,
            return_object_dump=args.json,
            region=args.region,
            brief=args.brief,
            resources_dir=resources_dir,
            worker_resources_subpath=worker_resources_subpath,
            **extra_args
            )

        if output is not None and args.run is None:
            print(json.dumps(output))

        if args.dry_run:
            return None

        return output['id']

    else:
        # REMOTE BUILD
        # The following flags might be useful in conjunction with
        # --remote. To enable these, we need to learn how to pass these
        # options through to the interior call of dx_build_app(let).
        if args.dry_run:
            parser.error('--remote and --repository cannot be combined with --dry-run')
        if args.overwrite:
            parser.error('--remote and --repository cannot be combined with --overwrite/-f')
        if args.archive:
            parser.error('--remote and --repository cannot be combined with --archive/-a')

        # The following flags are probably not useful in conjunction
        # with --remote.
        if args.json:
            parser.error('--remote and --repository cannot be combined with --json')
        if not args.use_temp_build_project:
            parser.error('--remote and --repository cannot be combined with --no-temp-build-project')

        if isinstance(args.region, list) and len(args.region) > 1:
            parser.error('--region can only be specified once for remote builds')
        region = args.region[0] if args.region is not None else None

        more_kwargs = {}
        if args.version_override:
            more_kwargs['version_override'] = args.version_override
        if args.bill_to:
            more_kwargs['bill_to_override'] = args.bill_to
        if not args.version_autonumbering:
            more_kwargs['do_version_autonumbering'] = False
        if not args.update:
            more_kwargs['do_try_update'] = False
        if not args.parallel_build:
            more_kwargs['do_parallel_build'] = False
        if not args.check_syntax:
            more_kwargs['do_check_syntax'] = False
        if args.nextflow and args.repository is not None:
            return build_pipeline_from_repository(args.repository, args.tag, args.profile, args.git_credentials, args.brief, extra_args)

        app_json = _parse_app_spec(source_dir)
        _check_suggestions(app_json, publish=args.publish)
        _verify_app_source_dir(source_dir, args.mode)
        if args.mode == "app" and not args.dry_run:
            dxpy.executable_builder.verify_developer_rights('app-' + app_json['name'])

        return _build_app_remote(args.mode, source_dir, destination_override=args.destination,
                                 publish=args.publish,
                                 region=region, watch=args.watch, **more_kwargs)


def build(args):
    try:
        executable_id = _build_app(args, json.loads(args.extra_args) if args.extra_args else {})
    except dxpy.app_builder.AppBuilderException as e:
        # AppBuilderException represents errors during app building
        # that could reasonably have been anticipated by the user.
        print("Error: %s" % (e.args,), file=sys.stderr)
        sys.exit(3)
    except dxpy.exceptions.DXAPIError as e:
        print("Error: %s" % (e,), file=sys.stderr)
        sys.exit(3)
    if args.run is not None:
        if executable_id is None:
            raise AssertionError('Expected executable_id to be set here')

        try:
            subprocess.check_call(['dx', 'run', executable_id, '--priority', 'high'] + args.run)
        except subprocess.CalledProcessError as e:
            sys.exit(e.returncode)
        except:
            err_exit()


def main(**kwargs):
    """
    Entry point for dx-build-app(let).

    Don't call this function as a subroutine in your program! It is liable to
    sys.exit your program when it detects certain error conditions, so you
    can't recover from those as you could if it raised exceptions. Instead,
    call dx_build_app.build_and_upload_locally which provides the real
    implementation for dx-build-app(let) but is easier to use in your program.
    """

    if len(sys.argv) > 0:
        if sys.argv[0].endswith('dx-build-app'):
            logging.warn('Warning: dx-build-app has been replaced with "dx build --create-app". Please update your scripts.')
        elif sys.argv[0].endswith('dx-build-applet'):
            logging.warn('Warning: dx-build-applet has been replaced with "dx build". Please update your scripts.')
        exit(0)

if __name__ == '__main__':
    main()
