# Copyright (C) 2013-2014 DNAnexus, Inc.
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

"""
Utilities used in the DNAnexus execution environment and test harness.
"""

from __future__ import print_function, unicode_literals

import os, sys, json, re, collections, logging, argparse, string, itertools, subprocess, tempfile
from functools import wraps
from collections import namedtuple

import dxpy
from ..compat import USING_PYTHON2, open
from ..exceptions import AppInternalError

ENTRY_POINT_TABLE = {}

RUN_COUNT = 0

# Locale-independent version of string.printable
ASCII_PRINTABLE = string.ascii_letters + string.digits + string.punctuation + string.whitespace
def _safe_unicode(o):
    """
    Returns an equivalent unicode object, trying harder to avoid
    dependencies on the Python default encoding.
    """
    def clean(s):
        return u''.join([c if c in ASCII_PRINTABLE else '?' for c in s])
    if USING_PYTHON2:
        try:
            return unicode(o)
        except:
            try:
                s = str(o)
                try:
                    return s.decode("utf-8")
                except:
                    return clean(s[:2048]) + u" [Raw error message: " + unicode(s.encode("hex"), 'utf-8') + u"]"
            except:
                return u"(Unable to decode Python exception message)"
    else:
        return str(o)

def _format_exception_message(e):
    """
    Formats the specified exception.
    """
    # Prevent duplication of "AppError" in places that print "AppError"
    # and then this formatted string
    if isinstance(e, dxpy.AppError):
        return _safe_unicode(e)
    if USING_PYTHON2:
        return unicode(e.__class__.__name__, 'utf-8') + ": " + _safe_unicode(e)
    else:
        return e.__class__.__name__ + ": " + _safe_unicode(e)


def run(function_name=None, function_input=None):
    """Triggers the execution environment entry point processor.

    Use this function in the program entry point code:

    .. code-block:: python

       import dxpy

       @dxpy.entry_point('main')
       def hello(i):
           pass

       dxpy.run()

    This method may be used to invoke the program either in a production
    environment (inside the execution environment) or for local
    debugging (in the debug harness), as follows:

    If the environment variable *DX_JOB_ID* is set, the processor
    retrieves the job with that ID from the API server. The job's
    *function* field indicates the function name to be invoked. That
    function name is looked up in the table of all methods decorated
    with *@dxpy.entry_point('name')* in the module from which
    :func:`run()` was called, and the matching method is invoked (with
    the job's input supplied as parameters). This is the mode of
    operation used in the DNAnexus execution environment.

    .. warning::

       The parameters *function_name* and *function_input* are
       disregarded in this mode of operation.

    If the environment variable *DX_JOB_ID* is not set, the function
    name may be given in *function_name*; if not set, it is set by the
    environment variable *DX_TEST_FUNCTION*. The function input may be
    given in *function_input*; if not set, it is set by the local file
    *job_input.json* which is expected to be present.

    The absence of *DX_JOB_ID* signals to :func:`run()` that execution
    is happening in the debug harness. In this mode of operation, all
    calls to :func:`dxpy.bindings.dxjob.new_dxjob()` (and higher level
    handler methods which use it) are intercepted, and :func:`run()` is
    invoked instead with appropriate inputs.

    """

    global RUN_COUNT
    RUN_COUNT += 1

    dx_working_dir = os.getcwd()

    if dxpy.JOB_ID is not None:
        logging.basicConfig()

        try:
            logging.getLogger().addHandler(dxpy.DXLogHandler())
        except dxpy.exceptions.DXError:
            print("TODO: FIXME: the EE client should die if logging is not available")

        job = dxpy.describe(dxpy.JOB_ID)
    else:
        if function_name is None:
            function_name = os.environ.get('DX_TEST_FUNCTION', 'main')
        if function_input is None:
            with open("job_input.json", "r") as fh:
                function_input = json.load(fh)

        job = {'function': function_name, 'input': function_input}

    with open("job_error_reserved_space", "w") as fh:
        fh.write("This file contains reserved space for writing job errors in case the filesystem becomes full.\n" + " "*1024*64)

    print("Invoking", job.get('function'), "with", job.get('input'))

    try:
        result = ENTRY_POINT_TABLE[job['function']](**job['input'])
    except dxpy.AppError as e:
        save_error(e, dx_working_dir, error_type="AppError")
        raise
    except Exception as e:
        save_error(e, dx_working_dir)
        raise

    if result is not None:
        # TODO: protect against client removing its original working directory
        os.chdir(dx_working_dir)
        with open("job_output.json", "wb") as fh:
            json.dump(result, fh, indent=2, cls=DXJSONEncoder)
            fh.write(b"\n")

    return result

def save_error(e, working_dir, error_type="AppInternalError"):
    if dxpy.JOB_ID is not None:
        os.chdir(working_dir)
        try:
            os.unlink("job_error_reserved_space")
        except:
            pass
        with open("job_error.json", "wb") as fh:
            json.dump({"error": {"type": error_type, "message": _format_exception_message(e)}}, fh)
            fh.write(b"\n")

# TODO: make this less naive with respect to cycles and any other things json.dumps() can handle
def convert_handlers_to_dxlinks(x):
    if isinstance(x, dxpy.DXObject):
        x = dxpy.dxlink(x)
    elif isinstance(x, collections.Mapping):
        for key, value in x.items():
            x[key] = convert_handlers_to_dxlinks(value)
    elif isinstance(x, list):
        for i in range(len(x)):
            x[i] = convert_handlers_to_dxlinks(x[i])
    return x

def parse_args_as_job_input(args, app_spec):
    parser = argparse.ArgumentParser()
    json_inputs = set()
    for ispec in app_spec.get("inputSpec", []):
        kwargs = {}
        if ispec.get("type") == "int":
            kwargs["type"] = int
        elif ispec.get("type") == "float":
            kwargs["type"] = float
        elif ispec.get("type") == "boolean":
            kwargs["type"] = bool
        elif ispec.get("type") != "string":
            json_inputs.add(ispec["name"])

        if ispec.get("optional") != None:
            kwargs["required"] = not ispec["optional"]

        parser.add_argument("--" + ispec["name"], **kwargs)

    inputs = {}
    for i, value in vars(parser.parse_args(args)).items():
        if value is None:
            continue
        if i in json_inputs:
            try:
                inputs[i] = json.loads(value)
            except ValueError:
                from dxpy.utils.resolver import resolve_existing_path
                project, path, results = resolve_existing_path(value, ask_to_resolve=False, describe={'id': True}, allow_mult=False)
                print(project, path, results)
                if results is None or len(results) != 1:
                    raise ValueError("Value {v} could not be resolved".format(v=value))
                inputs[i] = dxpy.dxlink(results[0]['id'], project_id=project)
        else:
            inputs[i] = value

    return inputs

def entry_point(entry_point_name):
    """Use this to decorate a DNAnexus execution environment entry point.

    Example:

    .. code-block:: python

       @dxpy.entry_point('main')
       def hello(i):
           pass

    """
    def wrap(f):
        ENTRY_POINT_TABLE[entry_point_name] = f
        @wraps(f)
        def wrapped_f(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapped_f
    return wrap

class DXJSONEncoder(json.JSONEncoder):
    """
    Like json.JSONEncoder, but converts DXObject objects into dxlinks.
    """
    def default(self, obj):
        if isinstance(obj, dxpy.DXObject):
            return dxpy.dxlink(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

class DXExecDependencyError(AppInternalError):
    pass

class DXExecDependencyInstaller(object):
    """
    Installs dependencies specified by the job.

    Dependencies are processed in the order specified in the
    bundledDepends, execDepends, and dependencies arrays of the
    runSpec hash (the former two are deprecated). Neighboring package
    dependencies of the same type are grouped.
    """
    group_pms = ("apt", "gem", "cpan", "cran")

    def __init__(self, executable_desc, job_desc, logger=None):
        if "runSpec" not in executable_desc:
            raise DXExecDependencyError('Expected field "runSpec" to be present in executable description"')

        self.exec_desc = executable_desc
        self.run_spec = executable_desc["runSpec"]
        self.job_desc = job_desc
        self.stage = self.job_desc.get("function", "main")
        self.logger = logger

        self.dep_groups = []
        for dep in itertools.chain(self.run_spec.get("bundledDepends", []),
                                   self.run_spec.get("execDepends", []),
                                   self.run_spec.get("dependencies", [])):
            self._validate_dependency(dep)
            if "stages" in dep and self.stage not in dep["stages"]:
                self.log("Skipping dependency {} because it is inactive in stage (function) {}".format(dep["name"],
                                                                                                       self.stage))
                continue

            dep_type = self._get_dependency_type(dep)
            if len(self.dep_groups) == 0 or self.dep_groups[-1]["type"] != dep_type or dep_type not in self.group_pms:
                self.dep_groups.append({"type": dep_type, "deps": [], "index": len(self.dep_groups)})
            self.dep_groups[-1]["deps"].append(dep)

    def log(self, message):
        if self.logger:
            self.logger.info(message)
        else:
            print(message)

    def generate_shellcode(self, dep_group):
        base_apt_shellcode = "apt-get install --yes --no-install-recommends {p}"
        dx_apt_update_shellcode = "apt-get update -o Dir::Etc::sourcelist=sources.list.d/nucleus.list -o Dir::Etc::sourceparts=- -o APT::Get::List-Cleanup=0"
        change_apt_archive = r"sed -i -e s?http://.*.ec2.archive.ubuntu.com?http://us.archive.ubuntu.com? /etc/apt/sources.list"
        apt_err_msg = "APT failed, retrying with full update against ubuntu.com"
        apt_shellcode_template = "({dx_upd} && {inst}) || (echo {e}; {change_apt_archive} && apt-get update && {inst})"
        apt_shellcode = apt_shellcode_template.format(dx_upd=dx_apt_update_shellcode,
                                                      change_apt_archive=change_apt_archive,
                                                      inst=base_apt_shellcode,
                                                      e=apt_err_msg)
        def make_pm_atoms(packages, version_separator="="):
            package_atoms = (p["name"] + (version_separator+p["version"] if "version" in p else "") for p in packages)
            return " ".join(map(str, package_atoms))

        dep_type, packages = dep_group["type"], dep_group["deps"]
        if dep_type == "apt":
            return apt_shellcode.format(p=make_pm_atoms(packages))
        elif dep_type == "pip":
            return "pip install --upgrade " + make_pm_atoms(packages, version_separator="==")
        elif dep_type == "gem":
            commands = []
            for p in packages:
                commands.append("gem install " + p["name"])
                if "version" in p:
                    commands[-1] += " --version " + p["version"]
            return " && ".join(map(str, commands))
        elif dep_type == "cpan":
            return "cpanm --notest " + make_pm_atoms(packages, version_separator="~")
        elif dep_type == "cran":
            r_shellcode = "R -e 'die <- function() { q(status=1) }; options(error=die); options(warn=2); install.packages(commandArgs(trailingOnly=TRUE), repos=\"http://cran.us.r-project.org\")' --args "
            return r_shellcode + make_pm_atoms(packages)
        elif dep_type == "git":
            commands = ["apt-get install --yes git make", "export GIT_SSH=dx-git-ssh-helper"]
            for dep in packages:
                subcommands = []
                build_dir = str(dep.get("destdir", "$(mktemp -d)"))
                subcommands.append("mkdir -p %s" % build_dir)
                subcommands.append("cd %s" % build_dir)
                subcommands.append("git clone " + str(dep["url"]))
                subdir = re.search("([^\/]+)$", str(dep["url"])).group(1)
                if subdir.endswith(".git"):
                    subdir = subdir[:-len(".git")]
                subcommands.append("cd '%s'" % subdir)
                if "tag" in dep:
                    subcommands.append("git checkout " + str(dep["tag"]))
                if "build_commands" in dep:
                    subcommands.append(str(dep["build_commands"]))
                commands.append("(" + " && ".join(subcommands) + ")")
            return " && ".join(commands)
        else:
            raise DXExecDependencyError("Package manager type {pm} not supported".format(pm=dep_type))

    def run(self, cmd, log_fh=None):
        subprocess.check_call(cmd, shell=True, stdout=log_fh, stderr=log_fh)

    def _install_dep_group(self, dep_group):
        self.log("Installing {} packages {}".format(dep_group["type"],
                                                    ", ".join(dep["name"] for dep in dep_group["deps"])))
        cmd = self.generate_shellcode(dep_group)
        log_filename = os.path.join(tempfile.gettempdir(), "dx_{type}_install_{index}.log".format(**dep_group))

        try:
            with open(log_filename, "w") as fh:
                self.run(cmd, log_fh=fh)
        except subprocess.CalledProcessError as e:
            with open(log_filename) as fh:
                sys.stdout.write(fh.read())
            raise DXExecDependencyError("Error while installing {type} packages {deps}".format(**dep_group))

    def _install_dep_bundle(self, bundle):
        if bundle["id"].get("$dnanexus_link", "").startswith("file-"):
            self.log("Downloading bundled file {name}".format(**bundle))
            dxpy.download_dxfile(bundle["id"], bundle["name"])
            self.run("dx-unpack '{}'".format(bundle["name"]))
        else:
            self.log('Skipping bundled dependency "{name}" because it does not refer to a file'.format(**bundle))

    def install(self):
        for dep_group in self.dep_groups:
            if dep_group["type"] == "bundle":
                self._install_dep_bundle(dep_group["deps"][0])
            else:
                self._install_dep_group(dep_group)

    def _validate_dependency(self, dep):
        if "name" not in dep:
            raise DXExecDependencyError('Expected field "name" to be present in execution dependency "{}"'.format(dep))
        if dep.get("package_manager") == "cran" and "version" in dep:
            msg = 'Execution dependency {} has a "version" field, but versioning is not supported for CRAN dependencies'
            raise DXExecDependencyError(msg.format(dep))
        elif dep.get("package_manager") == "git" and "url" not in dep:
            raise DXExecDependencyError('Execution dependency "{}" does not have a "url" field'.format(dep))

    def _get_dependency_type(self, dep):
        if "id" in dep:
            return "bundle"
        else:
            return dep.get("package_manager", "apt")
