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

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, json, subprocess, tempfile, multiprocessing
import datetime
import gzip
import hashlib
import io
import tarfile
import stat

import dxpy
import dxpy.executable_builder
from . import logger
from .utils import merge
from .utils.printing import fill
from .compat import input
from .cli import INTERACTIVE_CLI

NUM_CORES = multiprocessing.cpu_count()

DX_TOOLKIT_PKGS = ('dx-toolkit',)
DX_TOOLKIT_GIT_URLS = ("git@github.com:dnanexus/dx-toolkit.git",)


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

def is_link_local(link_target):
    """
    :param link_target: The target of a symbolic link, as given by os.readlink()
    :type link_target: string
    :returns: A boolean indicating the link is local to the current directory.
              This is defined to mean that os.path.isabs(link_target) == False
              and the link NEVER references the parent directory, so
              "./foo/../../curdir/foo" would return False.
    :rtype: boolean
    """
    is_local=(not os.path.isabs(link_target))

    if is_local:
        # make sure that the path NEVER extends outside the resources directory!
        d,l = os.path.split(link_target)
        link_parts = []
        while l:
            link_parts.append(l)
            d,l = os.path.split(d)
        curr_path = os.sep

        for p in reversed(link_parts):
            is_local = (is_local and not (curr_path == os.sep and p == os.pardir) )
            curr_path = os.path.abspath(os.path.join(curr_path, p))

    return is_local

def _fix_perms(perm_obj):
    """
    :param perm_obj: A permissions object, as given by os.stat()
    :type perm_obj: integer
    :returns: A permissions object that is the result of "chmod a+rX" on the
              given permission object.  This is defined to be the permission object
              bitwise or-ed with all stat.S_IR*, and if the stat.S_IXUSR bit is
              set, then the permission object should also be returned bitwise or-ed
              with stat.S_IX* (stat.S_IXUSR not included because it would be redundant).
    :rtype: integer
    """
    ret_perm = perm_obj | stat.S_IROTH | stat.S_IRGRP | stat.S_IRUSR
    if ret_perm & stat.S_IXUSR:
        ret_perm = ret_perm | stat.S_IXGRP | stat.S_IXOTH

    return ret_perm

def _fix_perm_filter(tar_obj):
    """
    :param tar_obj: A TarInfo object to be added to a tar file
    :tpye tar_obj: tarfile.TarInfo
    :returns: A TarInfo object with permissions changed (a+rX)
    :rtype: tarfile.TarInfo
    """
    tar_obj.mode = _fix_perms(tar_obj.mode)
    return tar_obj


def upload_resources(src_dir, project=None, folder='/', ensure_upload=False, force_symlinks=False, brief=False, resources_dir=None, worker_resources_subpath=""):
    """
    :param ensure_upload: If True, will bypass checksum of resources directory
                          and upload resources bundle unconditionally;
                          will NOT be able to reuse this bundle in future builds.
                          Else if False, will compute checksum and upload bundle
                          if checksum is different from a previously uploaded
                          bundle's checksum.
    :type ensure_upload: boolean
    :param force_symlinks: If true, will bypass the attempt to dereference any
                           non-local symlinks and will unconditionally include
                           the link as-is.  Note that this will almost certainly
                           result in a broken link within the resource directory
                           unless you really know what you're doing.
    :type force_symlinks: boolean
    :param resources_dir: Directory with resources to be archived and uploaded. If not given, uses `resources/`.
    :type resources_dir: str
    :param worker_resources_subpath: Path that will be prepended to the default directory where files are extracted on the worker.
                                     Default is empty string, therefore files would be extracted directly to the root folder.
                                     Example: If "home/dnanexus" is given, files will be extracted into /home/dnanexus.
    :type worker_resources_subpath: str
    :returns: A list (possibly empty) of references to the generated archive(s)
    :rtype: list

    If resources_dir exists, archives and uploads the contents of the resources_dir
    (usually ``resources/``) subdirectory of *src_dir* to a new remote file
    object, and returns a list describing a single bundled dependency in
    the form expected by the ``bundledDepends`` field of a run
    specification. Returns an empty list, if no archive was created.
    """
    if not resources_dir:
        resources_dir = os.path.join(src_dir, "resources")

    applet_spec = _get_applet_spec(src_dir)

    if project is None:
        dest_project = applet_spec['project']
    else:
        dest_project = project
        applet_spec['project'] = project

    if os.path.exists(resources_dir) and len(os.listdir(resources_dir)) > 0:
        target_folder = applet_spec['folder'] if 'folder' in applet_spec else folder

        # While creating the resource bundle, optimistically look for a
        # resource bundle with the same contents, and reuse it if possible.
        # The resource bundle carries a property 'resource_bundle_checksum'
        # that indicates the checksum; the way in which the checksum is
        # computed is given below.   If the checksum matches  (and
        # ensure_upload is False), then we will use the existing file,
        # otherwise, we will compress and upload the tarball.


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
        #
        # Note when looking at a link, if the link is to be dereferenced,
        # the mtime and mode used are that of the target (using os.stat())
        # If the link is to be kept as a link, the mtime and mode are those
        # of the link itself (using os.lstat())

        with tempfile.NamedTemporaryFile(suffix=".tar") as tar_tmp_fh:

            output_sha1 = hashlib.sha1()
            tar_fh = tarfile.open(fileobj=tar_tmp_fh, mode='w')

            for dirname, subdirs, files in os.walk(resources_dir):
                if not dirname.startswith(resources_dir):
                    raise AssertionError('Expected %r to start with root directory %r' % (dirname, resources_dir))

                # Add an entry for the directory itself
                relative_dirname = dirname[len(resources_dir):]
                dir_stat = os.lstat(dirname)
                if not relative_dirname.startswith('/'):
                    relative_dirname = '/' + relative_dirname

                fields = [relative_dirname, str(_fix_perms(dir_stat.st_mode)), str(int(dir_stat.st_mtime * 1000))]
                output_sha1.update(b''.join(s.encode('utf-8') + b'\0' for s in fields))

                # add an entry in the tar file for the current directory, but
                # do not recurse!
                tar_fh.add(dirname, arcname=worker_resources_subpath + relative_dirname, recursive=False, filter=_fix_perm_filter)
                # Canonicalize the order of subdirectories; this is the order in
                # which they will be visited by os.walk
                subdirs.sort()

                # check the subdirectories for symlinks.  We should throw an error
                # if there are any links that point outside of the directory (unless
                # --force-symlinks is given).  If a link is pointing internal to
                # the directory (or --force-symlinks is given), we should add it
                # as a file.
                for subdir_name in subdirs:
                    dir_path = os.path.join(dirname, subdir_name)

                    # If we do have a symlink,
                    if os.path.islink(dir_path):
                        # Let's get the pointed-to path to ensure that it is
                        # still in the directory
                        link_target = os.readlink(dir_path)

                        # If this is a local link, add it to the list of files (case 1)
                        # else raise an error
                        if force_symlinks or is_link_local(link_target):
                            files.append(subdir_name)
                        else:
                            raise AppBuilderException("Cannot include symlinks to directories outside of the resource directory.  '%s' points to directory '%s'" % (dir_path, os.path.realpath(dir_path)))


                # Canonicalize the order of files so that we compute the
                # checksum in a consistent order
                for filename in sorted(files):
                    deref_link = False

                    relative_filename = os.path.join(relative_dirname, filename)
                    true_filename = os.path.join(dirname, filename)

                    file_stat = os.lstat(true_filename)
                    # check for a link here, please!
                    if os.path.islink(true_filename):

                        # Get the pointed-to path
                        link_target = os.readlink(true_filename)

                        if not (force_symlinks or is_link_local(link_target)):
                            # if we are pointing outside of the directory, then:
                            # try to get the true stat of the file and make sure
                            # to dereference the link!
                            try:
                                file_stat = os.stat(os.path.join(dirname, link_target))
                                deref_link = True
                            except OSError:
                                # uh-oh! looks like we have a broken link!
                                # since this is guaranteed to cause problems (and
                                # we know we're not forcing symlinks here), we
                                # should throw an error
                                raise AppBuilderException("Broken symlink: Link '%s' points to '%s', which does not exist" % (true_filename, os.path.realpath(true_filename)) )


                    fields = [relative_filename, str(_fix_perms(file_stat.st_mode)), str(int(file_stat.st_mtime * 1000))]
                    output_sha1.update(b''.join(s.encode('utf-8') + b'\0' for s in fields))

                    # If we are to dereference, use the target fn
                    if deref_link:
                        true_filename = os.path.realpath(true_filename)
                    tar_fh.add(true_filename, arcname=worker_resources_subpath + relative_filename, filter=_fix_perm_filter)
                # end for filename in sorted(files)

            # end for dirname, subdirs, files in os.walk(resources_dir):

            # at this point, the tar is complete, so close the tar_fh
            tar_fh.close()

            # Optimistically look for a resource bundle with the same
            # contents, and reuse it if possible. The resource bundle
            # carries a property 'resource_bundle_checksum' that indicates
            # the checksum; the way in which the checksum is computed is
            # given in the documentation of _directory_checksum.

            if ensure_upload:
                properties_dict = {}
                existing_resources = False
            else:
                directory_checksum = output_sha1.hexdigest()
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
                if not brief:
                    logger.info("Found existing resource bundle that matches local resources directory: " +
                                existing_resources.get_id())

                dx_resource_archive = existing_resources
            else:

                logger.debug("Uploading in " + src_dir)
                # We need to compress the tar that we've created


                targz_fh = tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False)

                # compress the file by reading the tar file and passing
                # it though a GzipFile object, writing the given
                # block size (by default 8192 bytes) at a time
                targz_gzf = gzip.GzipFile(fileobj=targz_fh, mode='wb')
                tar_tmp_fh.seek(0)
                dat = tar_tmp_fh.read(io.DEFAULT_BUFFER_SIZE)
                while dat:
                    targz_gzf.write(dat)
                    dat = tar_tmp_fh.read(io.DEFAULT_BUFFER_SIZE)

                targz_gzf.flush()
                targz_gzf.close()
                targz_fh.close()

                if 'folder' in applet_spec:
                    try:
                        dxpy.get_handler(dest_project).new_folder(applet_spec['folder'], parents=True)
                    except dxpy.exceptions.DXAPIError:
                        pass # TODO: make this better

                dx_resource_archive = dxpy.upload_local_file(
                    targz_fh.name,
                    wait_on_close=True,
                    project=dest_project,
                    folder=target_folder,
                    hidden=True,
                    properties=properties_dict
                )

                os.unlink(targz_fh.name)

                # end compressed file creation and upload

            archive_link = dxpy.dxlink(dx_resource_archive.get_id())

        # end tempfile.NamedTemporaryFile(suffix=".tar") as tar_fh

        return [{'name': 'resources.tar.gz', 'id': archive_link}]
    else:
        return []


def upload_applet(src_dir, uploaded_resources, check_name_collisions=True, overwrite=False, archive=False,
                  project=None, override_folder=None, override_name=None,
                  dry_run=False, brief=False, **kwargs):
    """
    Creates a new applet object.

    :param project: ID of container in which to create the applet.
    :type project: str, or None to use whatever is specified in dxapp.json
    :param override_folder: folder name for the resulting applet which, if specified, overrides that given in dxapp.json
    :type override_folder: str
    :param override_name: name for the resulting applet which, if specified, overrides that given in dxapp.json
    :type override_name: str

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

    applets_to_overwrite = []
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
                applets_to_overwrite.append(result['id'])
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
                if not brief:
                    logger.info("Archived applet %s to %s:\"%s/%s\"" % (result['id'], dest_project, archive_folder, new_name))
            else:
                raise AppBuilderException("An applet already exists at %s (id %s) and the --overwrite (-f) or --archive (-a) options were not given" % (destination_path, result['id']))

    # -----
    # Override various fields from the pristine dxapp.json

    # Carry region-specific values from regionalOptions into the main
    # runSpec
    applet_spec["runSpec"].setdefault("bundledDepends", [])
    applet_spec["runSpec"].setdefault("assetDepends", [])
    if not dry_run:
        region = dxpy.api.project_describe(dest_project, input_params={"fields": {"region": True}})["region"]

        # if regionalOptions contain at least one region, they must include
        # the region of the target project
        if len(applet_spec.get('regionalOptions', {})) != 0 and region not in applet_spec.get('regionalOptions', {}):
            err_mesg = "destination project is in region {} but \"regionalOptions\" do not contain this region. ".format(region)
            err_mesg += "Please, update your \"regionalOptions\" specification"
            raise AppBuilderException(err_mesg)

        regional_options = applet_spec.get('regionalOptions', {}).get(region, {})

        # We checked earlier that if region-specific values for the
        # fields below are given, the same fields are not also specified
        # in the top-level runSpec. So the operations below should not
        # result in any user-supplied settings being clobbered.

        if 'systemRequirements' in regional_options:
            applet_spec["runSpec"]["systemRequirements"] = regional_options['systemRequirements']

        if 'bundledDepends' in regional_options:
            applet_spec["runSpec"]["bundledDepends"].extend(regional_options["bundledDepends"])
        if 'assetDepends' in regional_options:
            applet_spec["runSpec"]["assetDepends"].extend(regional_options["assetDepends"])

    # Inline Readme.md and Readme.developer.md
    dxpy.executable_builder.inline_documentation_files(applet_spec, src_dir)

    # Inline the code of the program
    if "file" in applet_spec["runSpec"]:
        # Put it into runSpec.code instead
        with open(os.path.join(src_dir, applet_spec["runSpec"]["file"])) as code_fh:
            applet_spec["runSpec"]["code"] = code_fh.read()
            del applet_spec["runSpec"]["file"]

    # If this is applet requires a cluster, inline any bootstrapScript code that may be provided.
    # bootstrapScript is an *optional* clusterSpec parameter.
    # NOTE: assumes bootstrapScript is always provided as a filename
    if "systemRequirements" in applet_spec["runSpec"]:
        sys_reqs = applet_spec["runSpec"]["systemRequirements"]
        for entry_point in sys_reqs:
            try:
                bootstrap_script = os.path.join(src_dir, sys_reqs[entry_point]["clusterSpec"]["bootstrapScript"])
                with open(bootstrap_script) as code_fh:
                    sys_reqs[entry_point]["clusterSpec"]["bootstrapScript"] = code_fh.read()
            except KeyError:
                # either no "clusterSpec" or no "bootstrapScript" within "clusterSpec"
                continue
            except IOError:
                raise AppBuilderException("The clusterSpec \"bootstrapScript\" could not be read.")

    # Attach bundled resources to the app
    if uploaded_resources is not None:
        applet_spec["runSpec"]["bundledDepends"].extend(uploaded_resources)

    # Validate and process assetDepends
    asset_depends = applet_spec["runSpec"]["assetDepends"]
    if type(asset_depends) is not list or any(type(dep) is not dict for dep in asset_depends):
        raise AppBuilderException("Expected runSpec.assetDepends to be an array of objects")
    for asset in asset_depends:
        asset_project = asset.get("project", None)
        asset_folder = asset.get("folder", '/')
        asset_stages = asset.get("stages", None)
        if "id" in asset:
            asset_record = dxpy.DXRecord(asset["id"]).describe(fields={'details'}, default_fields=True)
        elif "name" in asset and asset_project is not None and "version" in asset:
            try:
                asset_record = dxpy.find_one_data_object(zero_ok=True, classname="record", typename="AssetBundle",
                                                         name=asset["name"], properties=dict(version=asset["version"]),
                                                         project=asset_project, folder=asset_folder, recurse=False,
                                                         describe={"defaultFields": True, "fields": {"details": True}},
                                                         state="closed", more_ok=False)
            except dxpy.exceptions.DXSearchError:
                msg = "Found more than one asset record that matches: name={0}, folder={1} in project={2}."
                raise AppBuilderException(msg.format(asset["name"], asset_folder, asset_project))
        else:
            raise AppBuilderException("Each runSpec.assetDepends element must have either {'id'} or "
                                      "{'name', 'project' and 'version'} field(s).")

        if asset_record:
            if "id" in asset:
                asset_details = asset_record["details"]
            else:
                asset_details = asset_record["describe"]["details"]
            if "archiveFileId" in asset_details:
                archive_file_id = asset_details["archiveFileId"]
            else:
                raise AppBuilderException("The required field 'archiveFileId' was not found in "
                                          "the details of the asset bundle %s " % asset_record["id"])
            archive_file_name = dxpy.DXFile(archive_file_id).describe()["name"]
            bundle_depends = {
                "name": archive_file_name,
                "id": archive_file_id
            }
            if asset_stages:
                bundle_depends["stages"] = asset_stages
            applet_spec["runSpec"]["bundledDepends"].append(bundle_depends)
            # If the file is not found in the applet destination project, clone it from the asset project
            if (not dry_run and
                    dxpy.DXRecord(dxid=asset_record["id"], project=dest_project).describe()["project"] != dest_project):
                dxpy.DXRecord(asset_record["id"], project=asset_record["project"]).clone(dest_project)
        else:
            raise AppBuilderException("No asset bundle was found that matched the specification %s"
                                      % (json.dumps(asset)))

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

    # Now it is permissible to delete the old applet(s), if any
    if applets_to_overwrite:
        if not brief:
            logger.info("Deleting applet(s) %s" % (','.join(applets_to_overwrite)))
        dxpy.DXProject(dest_project).remove_objects(applets_to_overwrite)

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


def create_app_multi_region(regional_options, app_name, src_dir, publish=False, set_default=False, billTo=None,
                            try_versions=None, try_update=True, confirm=True, inherited_metadata={}, brief=False):
    """
    Creates a new app object from the specified applet(s).

    :param regional_options: Region-specific options for the app. See
        https://documentation.dnanexus.com/developer/api/running-analyses/apps#api-method-app-new
        for details; this should contain keys for each region the app is
        to be enabled in, and for the values, a dict containing (at
        minimum) a key "applet" whose value is an applet ID for that
        region.
    :type regional_options: dict
    """
    return _create_app(dict(regionalOptions=regional_options), app_name, src_dir, publish=publish,
                       set_default=set_default, billTo=billTo, try_versions=try_versions, try_update=try_update,
                       confirm=confirm, inherited_metadata=inherited_metadata, brief=brief)


def create_app(applet_id, applet_name, src_dir, publish=False, set_default=False, billTo=None, try_versions=None,
               try_update=True, confirm=True, regional_options=None):
    """
    Creates a new app object from the specified applet.

    .. deprecated:: 0.204.0
       Use :func:`create_app_multi_region()` instead.

    """
    # In this case we don't know the region of the applet, so we use the
    # legacy API {"applet": applet_id} without specifying a region
    # specifically.
    return _create_app(dict(applet=applet_id), applet_name, src_dir, publish=publish, set_default=set_default,
                       billTo=billTo, try_versions=try_versions, try_update=try_update, confirm=confirm)


def _create_app(applet_or_regional_options, app_name, src_dir, publish=False, set_default=False, billTo=None,
                try_versions=None, try_update=True, confirm=True, inherited_metadata={}, brief=False):

    if src_dir:
        app_spec = _get_app_spec(src_dir)
        if not brief:
            logger.info("Will create app with spec: %s" % (json.dumps(app_spec),))
        # Inline Readme.md and Readme.developer.md
        dxpy.executable_builder.inline_documentation_files(app_spec, src_dir)
    else:
        app_spec = inherited_metadata

    app_spec.update(applet_or_regional_options, name=app_name)

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
            if not brief:
                logger.info("Created app " + app_id)
            # Success!
            break
        elif app_describe.get("published", 0) == 0:
            logger.debug('App %s/%s already exists and has not been published' % (app_spec["name"], version))
            app_id = _update_version(app_spec['name'], app_spec['version'], app_spec, try_update=try_update)
            if app_id is None:
                continue
            if not brief:
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
            logger.warn('the app was NOT made public as requested in the app spec. To make it so, run "dx add users app-%s PUBLIC".' % (app_spec["name"],))

        if authorized_users_to_remove:
            dxpy.api.app_remove_authorized_users(app_id, input_params={'authorizedUsers': list(authorized_users_to_remove)})

    elif not len(existing_authorized_users) and not brief:
        # Apps that had authorized users added by any other means will
        # not have this message printed.
        logger.warn('authorizedUsers is missing from the app spec. No one will be able to view or run the app except the app\'s developers.')

    if publish:
        dxpy.api.app_publish(app_id, input_params={'makeDefault': set_default})
    else:
        # If no versions of this app have ever been published, then
        # we'll set the "default" tag to point to the latest
        # (unpublished) version.
        no_published_versions = len(list(dxpy.find_apps(name=app_name, published=True, limit=1))) == 0
        if no_published_versions:
            dxpy.api.app_add_tags(app_id, input_params={'tags': ['default']})

    return app_id


def get_enabled_regions(app_spec, from_command_line):
    """Returns a list of the regions in which the app should be enabled.

    Also validates that app_spec['regionalOptions'], if supplied, is
    well-formed.

    :param app_spec: app specification
    :type app_spec: dict
    :param from_command_line: The regions specified on the command-line
      via --region
    :type from_command_line: list or None

    """
    enabled_regions = dxpy.executable_builder.get_enabled_regions('app', app_spec, from_command_line, AppBuilderException)

    if enabled_regions is not None and len(enabled_regions) == 0:
        raise AssertionError("This app should be enabled in at least one region")
    return enabled_regions
