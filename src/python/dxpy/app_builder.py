# Copyright (C) 2013-2015 DNAnexus, Inc.
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

'''
App Builder Library
+++++++++++++++++++

Contains utility methods useful for compiling and deploying applets and apps
onto the platform.

You can specify the destination project in the following ways (with the earlier
ones taking precedence):

* Supply the *project* argument to :func:`upload_resources()` or
  :func:`upload_applet()`.
* Supply the 'project' attribute in your ``dxapp.json``.
* Set the ``DX_WORKSPACE_ID`` environment variable (when running in a job context).

You can use the function :func:`get_destination_project` to determine
the effective destination project.

'''

from __future__ import (print_function, unicode_literals)

import os, sys, json, subprocess, tempfile, multiprocessing
import datetime
import hashlib

import dxpy
from . import logger
from .utils import merge
from .utils.printing import fill
from .compat import input
from .cli import INTERACTIVE_CLI

NUM_CORES = multiprocessing.cpu_count()

DX_TOOLKIT_PKGS = ['dx-toolkit', 'dx-toolkit-beta', 'dx-toolkit-unstable']
DX_TOOLKIT_GIT_URLS = ["git@github.com:dnanexus/dx-toolkit.git"]

class AppBuilderException(Exception):
    """
    This exception is raised by the methods in this module when app or applet
    building fails.
    """
    pass

def _validate_applet_spec(applet_spec):
    if 'runSpec' not in applet_spec:
        raise AppBuilderException("Required field 'runSpec' not found in dxapp.json")

def _validate_app_spec(app_spec):
    pass

def _get_applet_spec(src_dir):
    applet_spec_file = os.path.join(src_dir, "dxapp.json")
    with open(applet_spec_file) as fh:
        applet_spec = json.load(fh)

    _validate_applet_spec(applet_spec)
    if 'project' not in applet_spec:
        applet_spec['project'] = dxpy.WORKSPACE_ID
    return applet_spec

def _get_app_spec(src_dir):
    app_spec_file = os.path.join(src_dir, "dxapp.json")
    with open(app_spec_file) as fh:
        app_spec = json.load(fh)

    _validate_app_spec(app_spec)
    return app_spec

def build(src_dir, parallel_build=True):
    """
    Runs any build scripts that are found in the specified directory.

    In particular, runs ``./configure`` if it exists, followed by ``make -jN``
    if it exists (building with as many parallel tasks as there are CPUs on the
    system).
    """
    # TODO: use Gentoo or deb buildsystem
    config_script = os.path.join(src_dir, "configure")
    if os.path.isfile(config_script) and os.access(config_script, os.X_OK):
        logger.debug("Running ./configure in {cwd}".format(cwd=os.path.abspath(src_dir)))
        try:
            subprocess.check_call([config_script])
        except subprocess.CalledProcessError as e:
            raise AppBuilderException("./configure in target directory failed with exit code %d" % (e.returncode,))
    if os.path.isfile(os.path.join(src_dir, "Makefile")) \
        or os.path.isfile(os.path.join(src_dir, "makefile")) \
        or os.path.isfile(os.path.join(src_dir, "GNUmakefile")):
        if parallel_build:
            make_shortcmd = "make -j%d" % (NUM_CORES,)
        else:
            make_shortcmd = "make"
        logger.debug("Building with {make} in {cwd}".format(make=make_shortcmd, cwd=os.path.abspath(src_dir)))
        try:
            make_cmd = ["make", "-C", src_dir]
            if parallel_build:
                make_cmd.append("-j" + str(NUM_CORES))
            subprocess.check_call(make_cmd)
        except subprocess.CalledProcessError as e:
            raise AppBuilderException("%s in target directory failed with exit code %d" % (make_shortcmd, e.returncode))

def get_destination_project(src_dir, project=None):
    """
    :returns: Project ID where applet specified by src_dir would be written
    :rtype: str

    Returns the project ID where the applet specified in *src_dir* (or
    its associated resource bundles) would be written. This returns the
    same project that would be used by :func:`upload_resources()` and
    :func:`upload_applet()`, given the same *src_dir* and *project*
    parameters.
    """
    if project is not None:
        return project
    return _get_applet_spec(src_dir)['project']


def _directory_checksum(resources_dir):
    """Returns a hash of the contents of resources_dir.

    The hash is based purely on the names, mtimes, and modes of the
    files, and not on the contents or other metadata.

    """

    # The input to the SHA1 contains entries of the form (whitespace
    # only included here for readability):
    #
    # / \0 MODE \0 MTIME \0
    # /foo \0 MODE \0 MTIME \0
    # ...
    #
    # where there is one entry for each directory or file (order is
    # specified below), followed by a numeric representation of the
    # mode, and the mtime in milliseconds since the epoch.

    output_sha1 = hashlib.sha1()

    for dirname, subdirs, files in os.walk(resources_dir):
        if not dirname.startswith(resources_dir):
            raise AssertionError('Expected %r to start with root directory %r' % (dirname, resources_dir))

        # Add an entry for the directory itself
        relative_dirname = dirname[len(resources_dir):]
        dir_stat = os.lstat(dirname)
        if not relative_dirname.startswith('/'):
            relative_dirname = '/' + relative_dirname
        fields = [relative_dirname, str(dir_stat.st_mode), str(int(dir_stat.st_mtime * 1000))]
        output_sha1.update(b''.join(s.encode('utf-8') + b'\0' for s in fields))

        # Canonicalize the order of files so that we compute the
        # checksum in a consistent order
        for filename in sorted(files):
            relative_filename = os.path.join(relative_dirname, filename)
            true_filename = os.path.join(dirname, filename)

            file_stat = os.lstat(true_filename)
            fields = [relative_filename, str(file_stat.st_mode), str(int(file_stat.st_mtime * 1000))]
            output_sha1.update(b''.join(s.encode('utf-8') + b'\0' for s in fields))

        # Canonicalize the order of subdirectories; this is the order in
        # which they will be visited by os.walk
        subdirs.sort()

    return output_sha1.hexdigest()


def upload_resources(src_dir, project=None, folder='/', ensure_upload=False):
    """
    :param ensure_upload: If True, will bypass checksum of resources directory
                          and upload resources bundle unconditionally;
                          will NOT be able to reuse this bundle in future builds.
                          Else if False, will compute checksum and upload bundle
                          if checksum is different from a previously uploaded
                          bundle's checksum.
    :type ensure_upload: boolean
    :returns: A list (possibly empty) of references to the generated archive(s)
    :rtype: list

    If it exists, archives and uploads the contents of the
    ``resources/`` subdirectory of *src_dir* to a new remote file
    object, and returns a list describing a single bundled dependency in
    the form expected by the ``bundledDepends`` field of a run
    specification. Returns an empty list, if no archive was created.
    """
    applet_spec = _get_applet_spec(src_dir)

    if project is None:
        dest_project = applet_spec['project']
    else:
        dest_project = project
        applet_spec['project'] = project

    resources_dir = os.path.join(src_dir, "resources")
    if os.path.exists(resources_dir) and len(os.listdir(resources_dir)) > 0:
        target_folder = applet_spec['folder'] if 'folder' in applet_spec else folder

        # Optimistically look for a resource bundle with the same
        # contents, and reuse it if possible. The resource bundle
        # carries a property 'resource_bundle_checksum' that indicates
        # the checksum; the way in which the checksum is computed is
        # given in the documentation of _directory_checksum.

        if ensure_upload:
            properties_dict = {}
            existing_resources = False
        else:
            directory_checksum = _directory_checksum(resources_dir)
            properties_dict = dict(resource_bundle_checksum=directory_checksum)
            existing_resources = dxpy.find_one_data_object(
                project=dest_project,
                folder=target_folder,
                properties=dict(resource_bundle_checksum=directory_checksum),
                visibility='either',
                zero_ok=True,
                state='closed',
                return_handler=True
            )

        if existing_resources:
            logger.info("Found existing resource bundle that matches local resources directory: " +
                        existing_resources.get_id())

            dx_resource_archive = existing_resources
        else:
            logger.debug("Uploading in " + src_dir)

            with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tar_fh:
                # The directory contents may have changed since the
                # first time we checksummed the directory. Ideally we
                # would extract the tar file to determine the checksum
                # of the actually archived files, but maybe this is a
                # little too paranoid.
                subprocess.check_call(['tar', '-C', resources_dir, '-czf', tar_fh.name, '.'])
                if 'folder' in applet_spec:
                    try:
                        dxpy.get_handler(dest_project).new_folder(applet_spec['folder'], parents=True)
                    except dxpy.exceptions.DXAPIError:
                        pass # TODO: make this better
                dx_resource_archive = dxpy.upload_local_file(
                    tar_fh.name,
                    wait_on_close=True,
                    project=dest_project,
                    folder=target_folder,
                    hidden=True,
                    properties=properties_dict
                )

        archive_link = dxpy.dxlink(dx_resource_archive.get_id())
        return [{'name': 'resources.tar.gz', 'id': archive_link}]
    else:
        return []

def _inline_documentation_files(app_spec, src_dir):
    """
    Modifies the provided app_spec dict (which may be an app or applet
    spec, actually) to inline the contents of the readme file into
    "description" and the developer readme into "developerNotes".
    """
    # Inline description from a readme file
    if 'description' not in app_spec:
        readme_filename = None
        for filename in 'README.md', 'Readme.md', 'readme.md':
            if os.path.exists(os.path.join(src_dir, filename)):
                readme_filename = filename
                break
        if readme_filename is not None:
            with open(os.path.join(src_dir, readme_filename)) as fh:
                app_spec['description'] = fh.read()

    # Inline developerNotes from Readme.developer.md
    if 'developerNotes' not in app_spec:
        for filename in 'README.developer.md', 'Readme.developer.md', 'readme.developer.md':
            if os.path.exists(os.path.join(src_dir, filename)):
                with open(os.path.join(src_dir, filename)) as fh:
                    app_spec['developerNotes'] = fh.read()
                break

def upload_applet(src_dir, uploaded_resources, check_name_collisions=True, overwrite=False, archive=False, project=None, override_folder=None, override_name=None, dx_toolkit_autodep="stable", dry_run=False, **kwargs):
    """
    Creates a new applet object.

    :param project: ID of container in which to create the applet.
    :type project: str, or None to use whatever is specified in dxapp.json
    :param override_folder: folder name for the resulting applet which, if specified, overrides that given in dxapp.json
    :type override_folder: str
    :param override_name: name for the resulting applet which, if specified, overrides that given in dxapp.json
    :type override_name: str
    :param dx_toolkit_autodep: What type of dx-toolkit dependency to inject if none is present. "stable" for the APT package; "git" for HEAD of dx-toolkit master branch; or False for no dependency.
    :type dx_toolkit_autodep: boolean or string
    """
    applet_spec = _get_applet_spec(src_dir)

    if project is None:
        dest_project = applet_spec['project']
    else:
        dest_project = project
        applet_spec['project'] = project

    if 'name' not in applet_spec:
        try:
            applet_spec['name'] = os.path.basename(os.path.abspath(src_dir))
        except:
            raise AppBuilderException("Could not determine applet name from the specification (dxapp.json) or from the name of the working directory (%r)" % (src_dir,))

    if override_folder:
        applet_spec['folder'] = override_folder
    if 'folder' not in applet_spec:
        applet_spec['folder'] = '/'

    if override_name:
        applet_spec['name'] = override_name

    if 'dxapi' not in applet_spec:
        applet_spec['dxapi'] = dxpy.API_VERSION

    applet_to_overwrite = None
    archived_applet = None
    if check_name_collisions and not dry_run:
        destination_path = applet_spec['folder'] + ('/' if not applet_spec['folder'].endswith('/') else '') + applet_spec['name']
        logger.debug("Checking for existing applet at " + destination_path)
        for result in dxpy.find_data_objects(classname="applet", name=applet_spec["name"], folder=applet_spec['folder'], project=dest_project, recurse=False):
            if overwrite:
                # Don't remove the old applet until after the new one
                # has been created. This avoids a race condition where
                # we remove the old applet, but that causes garbage
                # collection of the bundled resources that will be
                # shared with the new applet
                applet_to_overwrite = result['id']
            elif archive:
                logger.debug("Archiving applet %s" % (result['id']))
                proj = dxpy.DXProject(dest_project)
                archive_folder = '/.Applet_archive'
                try:
                    proj.list_folder(archive_folder)
                except dxpy.DXAPIError:
                    proj.new_folder(archive_folder)

                proj.move(objects=[result['id']], destination=archive_folder)
                archived_applet = dxpy.DXApplet(result['id'], project=dest_project)
                now = datetime.datetime.fromtimestamp(archived_applet.created/1000).ctime()
                new_name = archived_applet.name + " ({d})".format(d=now)
                archived_applet.rename(new_name)
                logger.info("Archived applet %s to %s:\"%s/%s\"" % (result['id'], dest_project, archive_folder, new_name))
            else:
                raise AppBuilderException("An applet already exists at %s (id %s) and the --overwrite (-f) or --archive (-a) options were not given" % (destination_path, result['id']))

    # -----
    # Override various fields from the pristine dxapp.json

    # Inline Readme.md and Readme.developer.md
    _inline_documentation_files(applet_spec, src_dir)

    # Inline the code of the program
    if "runSpec" in applet_spec and "file" in applet_spec["runSpec"]:
        # Avoid using runSpec.file for now, it's not fully implemented
        #code_filename = os.path.join(src_dir, applet_spec["runSpec"]["file"])
        #f = dxpy.upload_local_file(code_filename, wait_on_close=True)
        #applet_spec["runSpec"]["file"] = f.get_id()
        # Put it into runSpec.code instead
        with open(os.path.join(src_dir, applet_spec["runSpec"]["file"])) as code_fh:
            applet_spec["runSpec"]["code"] = code_fh.read()
            del applet_spec["runSpec"]["file"]

    # Attach bundled resources to the app
    if uploaded_resources is not None:
        applet_spec["runSpec"].setdefault("bundledDepends", [])
        applet_spec["runSpec"]["bundledDepends"].extend(uploaded_resources)

    # Include the DNAnexus client libraries as an execution dependency, if they are not already
    # there
    if dx_toolkit_autodep == "git":
        dx_toolkit_dep = {"name": "dx-toolkit",
                          "package_manager": "git",
                          "url": "git://github.com/dnanexus/dx-toolkit.git",
                          "tag": "master",
                          "build_commands": "make install DESTDIR=/ PREFIX=/opt/dnanexus"}
    # TODO: reject "beta" and "unstable" eventually
    elif dx_toolkit_autodep in ("stable", "beta", "unstable"):
        dx_toolkit_dep = {"name": "dx-toolkit", "package_manager": "apt"}
    elif dx_toolkit_autodep:
        raise AppBuilderException("dx_toolkit_autodep must be one of 'stable', 'git', or False; got %r instead" % (dx_toolkit_autodep,))

    if dx_toolkit_autodep:
        applet_spec["runSpec"].setdefault("execDepends", [])
        exec_depends = applet_spec["runSpec"]["execDepends"]
        if type(exec_depends) is not list or any(type(dep) is not dict for dep in exec_depends):
            raise AppBuilderException("Expected runSpec.execDepends to be an array of objects")
        dx_toolkit_dep_found = any(dep.get('name') in DX_TOOLKIT_PKGS or dep.get('url') in DX_TOOLKIT_GIT_URLS for dep in exec_depends)
        if not dx_toolkit_dep_found:
            exec_depends.append(dx_toolkit_dep)
            if dx_toolkit_autodep == "git":
                applet_spec.setdefault("access", {})
                applet_spec["access"].setdefault("network", [])
                # Note: this can be set to "github.com" instead of "*" if the build doesn't download any deps
                if "*" not in applet_spec["access"]["network"]:
                    applet_spec["access"]["network"].append("*")

    merge(applet_spec, kwargs)

    # -----
    # Now actually create the applet

    if dry_run:
        print("Would create the following applet:")
        print(json.dumps(applet_spec, indent=2))
        print("*** DRY-RUN-- no applet was created ***")
        return None, None

    if applet_spec.get("categories", []):
        if "tags" not in applet_spec:
            applet_spec["tags"] = []
        applet_spec["tags"] = list(set(applet_spec["tags"]) | set(applet_spec["categories"]))

    applet_id = dxpy.api.applet_new(applet_spec)["id"]

    if archived_applet:
        archived_applet.set_properties({'replacedWith': applet_id})

    # Now it is permissible to delete the old applet, if any
    if applet_to_overwrite:
        logger.info("Deleting applet %s" % (applet_to_overwrite,))
        # TODO: test me
        dxpy.DXProject(dest_project).remove_objects([applet_to_overwrite])

    return applet_id, applet_spec


def _create_or_update_version(app_name, version, app_spec, try_update=True):
    """
    Creates a new version of the app. Returns an app_id, or None if the app has
    already been created and published.
    """
    # This has a race condition since the app could have been created or
    # published since we last looked.
    try:
        app_id = dxpy.api.app_new(app_spec)["id"]
        return app_id
    except dxpy.exceptions.DXAPIError as e:
        # TODO: detect this error more reliably
        if e.name == 'InvalidInput' and e.msg == 'Specified name and version conflict with an existing alias':
            print('App %s/%s already exists' % (app_spec["name"], version), file=sys.stderr)
            # The version number was already taken, so app/new doesn't work.
            # However, maybe it hasn't been published yet, so we might be able
            # to app-xxxx/update it.
            app_describe = dxpy.api.app_describe("app-" + app_name, alias=version)
            if app_describe.get("published", 0) > 0:
                return None
            return _update_version(app_name, version, app_spec, try_update=try_update)
        raise e

def _update_version(app_name, version, app_spec, try_update=True):
    """
    Updates a version of the app in place. Returns an app_id, or None if the
    app has already been published.
    """
    if not try_update:
        return None
    try:
        app_id = dxpy.api.app_update("app-" + app_name, version, app_spec)["id"]
        return app_id
    except dxpy.exceptions.DXAPIError as e:
        if e.name == 'InvalidState':
            print('App %s/%s has already been published' % (app_spec["name"], version), file=sys.stderr)
            return None
        raise e

def create_app(applet_id, applet_name, src_dir, publish=False, set_default=False, billTo=None, try_versions=None, try_update=True, confirm=True):
    """
    Creates a new app object from the specified applet.
    """
    app_spec = _get_app_spec(src_dir)
    logger.info("Will create app with spec: %s" % (app_spec,))

    app_spec["applet"] = applet_id
    app_spec["name"] = applet_name

    # Inline Readme.md and Readme.developer.md
    _inline_documentation_files(app_spec, src_dir)

    if billTo:
        app_spec["billTo"] = billTo
    if not try_versions:
        try_versions = [app_spec["version"]]

    for version in try_versions:
        logger.debug("Attempting to create version %s..." % (version,))
        app_spec['version'] = version
        app_describe = None
        try:
            # 404, which is rather likely in this app_describe request
            # (the purpose of the request is to find out whether the
            # version of interest exists), would ordinarily cause this
            # request to be retried multiple times, introducing a
            # substantial delay. So we disable retrying here for this
            # request.
            app_describe = dxpy.api.app_describe("app-" + app_spec["name"], alias=version, always_retry=False)
        except dxpy.exceptions.DXAPIError as e:
            if e.name == 'ResourceNotFound':
                pass
            else:
                raise e
        # Now app_describe is None if the app didn't exist, OR it contains the
        # app describe content.

        # The describe check does not eliminate race conditions since an app
        # may always have been created, or published, since we last looked at
        # it. So the describe that happens here is just to save time and avoid
        # unnecessary API calls, but we always have to be prepared to recover
        # from API errors.
        if app_describe is None:
            logger.debug('App %s/%s does not yet exist' % (app_spec["name"], version))
            app_id = _create_or_update_version(app_spec['name'], app_spec['version'], app_spec, try_update=try_update)
            if app_id is None:
                continue
            logger.info("Created app " + app_id)
            # Success!
            break
        elif app_describe.get("published", 0) == 0:
            logger.debug('App %s/%s already exists and has not been published' % (app_spec["name"], version))
            app_id = _update_version(app_spec['name'], app_spec['version'], app_spec, try_update=try_update)
            if app_id is None:
                continue
            logger.info("Updated existing app " + app_id)
            # Success!
            break
        else:
            logger.debug('App %s/%s already exists and has been published' % (app_spec["name"], version))
            # App has already been published. Give up on this version.
            continue
    else:
        # All versions requested failed
        if len(try_versions) != 1:
            tried_versions = 'any of the requested versions: ' + ', '.join(try_versions)
        else:
            tried_versions = 'the requested version: ' + try_versions[0]
        raise AppBuilderException('Could not create %s' % (tried_versions,))

    # Set categories appropriately.
    categories_to_set = app_spec.get("categories", [])
    existing_categories = dxpy.api.app_list_categories(app_id)['categories']
    categories_to_add = set(categories_to_set).difference(set(existing_categories))
    categories_to_remove = set(existing_categories).difference(set(categories_to_set))
    if categories_to_add:
        dxpy.api.app_add_categories(app_id, input_params={'categories': list(categories_to_add)})
    if categories_to_remove:
        dxpy.api.app_remove_categories(app_id, input_params={'categories': list(categories_to_remove)})

    # Set developers list appropriately, but only if provided.
    developers_to_set = app_spec.get("developers")
    if developers_to_set is not None:
        existing_developers = dxpy.api.app_list_developers(app_id)['developers']
        developers_to_add = set(developers_to_set) - set(existing_developers)
        developers_to_remove = set(existing_developers) - set(developers_to_set)

        skip_updating_developers = False
        if developers_to_add or developers_to_remove:
            parts = []
            if developers_to_add:
                parts.append('the following developers will be added: ' + ', '.join(sorted(developers_to_add)))
            if developers_to_remove:
                parts.append('the following developers will be removed: ' + ', '.join(sorted(developers_to_remove)))
            developer_change_message = '; and '.join(parts)
            if confirm:
                if INTERACTIVE_CLI:
                    try:
                        print('***')
                        print(fill('WARNING: ' + developer_change_message))
                        print('***')
                        value = input('Confirm updating developers list [y/N]: ')
                    except KeyboardInterrupt:
                        value = 'n'
                    if not value.lower().startswith('y'):
                        skip_updating_developers = True
                else:
                    # Default to NOT updating developers if operating
                    # without a TTY.
                    logger.warn('skipping requested change to the developer list. Rerun "dx build" interactively or pass --yes to confirm this change.')
                    skip_updating_developers = True
            else:
                logger.warn(developer_change_message)

        if not skip_updating_developers:
            if developers_to_add:
                dxpy.api.app_add_developers(app_id, input_params={'developers': list(developers_to_add)})
            if developers_to_remove:
                dxpy.api.app_remove_developers(app_id, input_params={'developers': list(developers_to_remove)})

    # Set authorizedUsers list appropriately, but only if provided.
    authorized_users_to_set = app_spec.get("authorizedUsers")
    existing_authorized_users = dxpy.api.app_list_authorized_users(app_id)['authorizedUsers']
    if authorized_users_to_set is not None:
        authorized_users_to_add = set(authorized_users_to_set) - set(existing_authorized_users)
        authorized_users_to_remove = set(existing_authorized_users) - set(authorized_users_to_set)

        skip_adding_public = False
        if 'PUBLIC' in authorized_users_to_add:
            acl_change_message = 'app-%s will be made public. Anyone will be able to view and run all published versions of this app.' % (app_spec['name'],)
            if confirm:
                if INTERACTIVE_CLI:
                    try:
                        print('***')
                        print(fill('WARNING: ' + acl_change_message))
                        print('***')
                        value = input('Confirm making this app public [y/N]: ')
                    except KeyboardInterrupt:
                        value = 'n'
                    if not value.lower().startswith('y'):
                        skip_adding_public = True
                else:
                    # Default to NOT adding PUBLIC if operating
                    # without a TTY.
                    logger.warn('skipping requested change to add PUBLIC to the authorized users list. Rerun "dx build" interactively or pass --yes to confirm this change.')
                    skip_adding_public = True
            else:
                logger.warn(acl_change_message)

        if skip_adding_public:
            authorized_users_to_add -= {'PUBLIC'}
        if authorized_users_to_add:
            dxpy.api.app_add_authorized_users(app_id, input_params={'authorizedUsers': list(authorized_users_to_add)})
        if skip_adding_public:
            logger.warn('the app was NOT made public as requested in the dxapp.json. To make it so, run "dx add users app-%s PUBLIC".' % (app_spec["name"],))

        if authorized_users_to_remove:
            dxpy.api.app_remove_authorized_users(app_id, input_params={'authorizedUsers': list(authorized_users_to_remove)})

    elif not len(existing_authorized_users):
        # Apps that had authorized users added by any other means will
        # not have this message printed.
        logger.warn('authorizedUsers is missing from the dxapp.json. No one will be able to view or run the app except the app\'s developers.')

    if publish:
        dxpy.api.app_publish(app_id, input_params={'makeDefault': set_default})
    else:
        # If no versions of this app have ever been published, then
        # we'll set the "default" tag to point to the latest
        # (unpublished) version.
        no_published_versions = len(list(dxpy.find_apps(name=applet_name, published=True, limit=1))) == 0
        if no_published_versions:
            dxpy.api.app_add_tags(app_id, input_params={'tags': ['default']})

    return app_id
