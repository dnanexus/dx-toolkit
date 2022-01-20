# Copyright (C) 2013-2021 DNAnexus, Inc.
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
import os
from typing import Optional, Tuple, Union

import dxpy
import dxpy.api
from dxpy.utils import resolver
from dxpy.sugar import get_log
from python.dxpy.bindings.dxproject import DXProject


LOG = get_log(__name__)


def get_project(
    project_desc: Union[str, dxpy.DXProject],
    level: str = "VIEW",
    exists: Optional[bool] = None,
    create: bool = False,
    region: Optional[str] = None,
    **kwargs,
) -> dxpy.DXProject:
    """
    Gets a project by name or ID. Creates a new project if there is no existing project with the
    given name and `create is True`.

    Args:
        project_desc: Project ID (project-XXX), name, or DXProject object. If this is a
            `dxpy.DXProject` object, it is validated and returned. If `None`, defaults to
            `dxpy.PROJECT_CONTEXT_ID`.
        level: Minimum access level to search.
        exists: Assert whether the project exists (`True`) or does not exist (`False`). If `None`,
            existance is not checked.
        create: Whether to create the project if it does not exist. Ignored if `project_desc` is a
            `dxpy.DXProject` object.
        region: Region in which to create the project.
        **kwargs: Additional keyword arguments to pass to `find_projects()` and `project_new()`.

    Returns:
        A `dxpy.DXProject` object, or `None` if the project does not exist and `create` is `False`.

    Raises:
        * dxpy.DXSearchError if the project is not in the expected region, or if the
            project exists but was not expected to, or if project is expected to exist
            and does not.
        * dxpy.exceptions.PermissionDenied if user does not have proper permissions
            to access the project.
    """
    project = None
    search = True

    if project_desc is None:
        project = dxpy.DXProject(dxpy.PROJECT_CONTEXT_ID)
        create = False
        search = False
    elif isinstance(project_desc, dxpy.DXProject):
        project = project_desc
        create = False
        search = False
    elif resolver.is_container_id(project_desc):
        project = dxpy.DXProject(project_desc)

    if project:
        LOG.info("Checking if project with ID %s exists", project_desc)
        try:
            project_region = project.describe()["region"]
            if region:
                if project_region != region:
                    raise dxpy.DXSearchError(
                        "Project {} region {} does not match expected region {}".format(
                            project.name, project_region, region
                        )
                    )
                else:
                    LOG.info(
                        "Project %s exists in region %s", project.name, project_region
                    )
        except dxpy.exceptions.ResourceNotFound:
            LOG.info("Project with ID %s not found", project.get_id())
            project = None

    if project is None and search:
        LOG.info("Searching for project with name %s", project_desc)
        find_args = dict(kwargs)
        if region:
            find_args["region"] = region
        if level:
            find_args["level"] = level
        project = dxpy.find_one_project(
            name=project_desc,
            zero_ok=True,
            more_ok=False,
            return_handler=True,
            **find_args,
        )

    if project:
        if exists is False:
            raise dxpy.DXSearchError(
                "Project {} exists but was expected not to exist".format(project_desc)
            )
        else:
            return project
    elif exists is True:
        raise dxpy.DXSearchError("Project {} does not exist".format(project_desc))
    elif create:
        LOG.info(
            "Creating project %s in %s region", project_desc, region or "<default>"
        )
        create_args = dict(kwargs)
        create_args["name"] = project_desc
        if region:
            create_args["region"] = region
        project_id = dxpy.api.project_new(create_args)["id"]
        return dxpy.DXProject(project_id)


def ensure_folder(
    folder: str,
    project: Optional[Union[str, dxpy.DXProject]] = None,
    exists: Optional[bool] = None,
    create: bool = False,
) -> list:
    """Checks that the given project contains (or does not contain) the given folder. The folder is
    created if it does not exist and `create is True`.

    Args:
        folder: The folder to validate.
        project: Project ID, name, or DXProject object. The project in which to look for the
            folder. Defaults to the current project.
        exists: Assert whether the folder exists (True) or does not exist (False).
        create: Whether to create the folder if it does not exist.

    Returns:
        A listing of the objects in the folder.

    Raises:
        dxpy.DXSearchError if the folder does exist and `exists` is `False` or if the folder does
        not exist and `exists` is `True`.
    """
    if folder is None:
        raise ValueError("'folder' cannot be None.")

    project = get_project(project)

    LOG.info(
        "Checking whether folder %s exists in project %s", folder, project.get_id()
    )

    try:
        ls = project.list_folder(folder=folder)
    except dxpy.exceptions.ResourceNotFound:
        ls = None

    if ls is None:
        if exists is True:
            LOG.error(
                "Folder %s not found in project %s",
                folder,
                project.get_id(),
            )
            raise dxpy.DXSearchError(
                f"Folder {folder} does not exist in project {project.get_id()}"
            )
        elif create:
            LOG.info("Creating new folder %s in project %s", folder, project.get_id())
            project.new_folder(folder, parents=True)
            return []
    elif exists is False:
        raise dxpy.DXSearchError(
            f"Folder {folder} exists in project {project.get_id()} but was expected not to exist"
        )
    else:
        return ls


def get_data_object(
    data_obj_desc: Union[str, dict, dxpy.DXDataObject],
    project: Optional[Union[str, dxpy.DXProject]] = None,
    classname: Optional[str] = None,
    exists: Optional[bool] = None,
    **kwargs,
) -> dxpy.DXDataObject:
    """
    Gets a data object by name or ID.

    Args:
        data_obj_desc: ID or name of the data object, a dxlink, or an instance of
            `dxpy.DXDataObject`.
        project: The project in which to get the data object. Defaults to the current project.
        classname: Classname of the data object; can be any data object class (e.g. "file",
            "record", or "database"), "*", or `None`. If either "*" or `None` is specified, the
            class of the data object is not validated when `data_obj_desc` is an ID or a `dxpy.
            DXDataObject` instance; the difference between the two is that when searching by name,
            "*" means don't specify the `classname` parameter and `None` means specify "file"
            as the `classname` parameter.
        exists: Assert whether the data object exists (`True`) or does not exist (`False`). If
            `None`, existance is not checked.
        **kwargs: Additional keyword arguments to use when searching for the data object.

    Returns:
        A `dxpy.DXDataObject` object.

    Raises:
        dxpy.DXSearchError if the data object does exist and `exists is False` or if the data
        object does not exist and `exists is True`.
    """
    if data_obj_desc is None:
        raise ValueError("'data_obj_desc' cannot be None")

    project = get_project(project)
    data_obj = None
    search = True

    if isinstance(data_obj_desc, dxpy.DXDataObject):
        data_obj = data_obj_desc
        search = False
    elif isinstance(data_obj_desc, dict):
        data_obj = dxpy.get_handler(data_obj_desc, project.get_id())
    elif resolver.is_data_obj_id(data_obj_desc):
        data_obj = dxpy.get_handler(data_obj_desc, project.get_id())

    if data_obj:
        try:
            data_obj.describe()
            if classname not in ("*", None):
                dxpy.verify_string_dxid(data_obj.get_id(), classname)
        except dxpy.DXError:
            data_obj = None

    if data_obj is None and search:
        folder, name, recurse = _parse_object_name(data_obj_desc)
        kwargs["return_handler"] = True
        if "folder" not in kwargs:
            kwargs["folder"] = folder
        if "recurse" not in kwargs:
            kwargs["recurse"] = recurse
        if classname is None:
            kwargs["classname"] = "file"
        elif classname != "*":
            kwargs["classname"] = classname
        data_obj = dxpy.find_one_data_object(
            zero_ok=True, more_ok=False, project=project.get_id(), name=name, **kwargs
        )

    if not data_obj and exists is True:
        raise dxpy.DXSearchError("Data object {} does not to exist", data_obj_desc)
    if data_obj and exists is False:
        raise dxpy.DXSearchError(
            "Data object {} exists but was expected not to exist", data_obj_desc
        )

    return data_obj


def get_workflow(
    workflow_desc: Union[str, dict, dxpy.DXWorkflow, dxpy.DXGlobalWorkflow],
    project: Optional[Union[str, dxpy.DXProject]] = None,
) -> Union[dxpy.DXWorkflow, dxpy.DXGlobalWorkflow]:
    """
    Searches for a project workflow or global workflow with the given ID or name. This is a
    convenience method for resolving a workflow when, for example, a user can provide either a
    project workflow or a global workflow as a paramter to an app.

    Args:
        workflow_desc: A workflow ID (either 'workflow-xxx' or 'globalworkflow-xxx') or name,
            a dxlink, or a `dxpy.DXWorkflow` or `dxpy.DXGlobalWorkflow` object.
        project: Project ID or `dxpy.DXProject` object. The project to search for the workflow; if
            not found here it is expected to be a global workflow.

    Returns:
        A `dxpy.DXWorkflow` or `dxpy.DXGlobalWorkflow    object.

    Raises:
        * dxpy.DXSearchError if the workflow does not exist.
        * dxpy.exceptions.PermissionDenied if user does not have proper permissions
            to access the workflow.
    """
    if workflow_desc is None:
        raise ValueError("'workflow_desc' cannot be None.")

    workflow = get_data_object(workflow_desc, project, classname="workflow")

    if not workflow:
        workflow = get_globalworkflow(workflow_desc)

    return workflow


def get_globalworkflow(
    workflow_desc: Union[str, dict, dxpy.DXGlobalWorkflow], **kwargs
) -> dxpy.DXGlobalWorkflow:
    """
    Gets the global workflow with the given ID or name.

    Args:
        workflow_desc: The globalworkflow ID or name, a dxlink, or a `dxpy.DXGlobalWorkflow`
            instance.
        kwargs: Additional kwargs to use when looking up the globalworkflow by name.

    Returns:
        A `dxpy.DXGlobalWorkflow` object.

    Raises:
        * dxpy.DXSearchError if the globalworkflow does not exist.
        * dxpy.exceptions.PermissionDenied if user does not have proper permissions
            to access the globalworkflow.
    """
    if workflow_desc is None:
        raise ValueError("'workflow_desc' cannot be None.")

    if isinstance(workflow_desc, dxpy.DXGlobalWorkflow):
        return workflow_desc

    if isinstance(workflow_desc, dict):
        return dxpy.get_handler(workflow_desc)

    if resolver.is_hashid(workflow_desc):
        try:
            LOG.info("Checking if globalworkflow with ID %s exists", workflow_desc)
            workflow = dxpy.DXGlobalWorkflow(workflow_desc)
            workflow.describe()
            return workflow
        except dxpy.DXError:
            pass

    if workflow_desc.startswith("globalworkflow-"):
        workflow_desc = workflow_desc[15:]

    kwargs["limit"] = 2
    candidates = dxpy.find_global_workflows(**kwargs)
    workflow = next(candidates, None)
    if workflow is None:
        raise dxpy.DXSearchError(f"Expected one result, but found none: {kwargs}")
    if next(candidates, None) is not None:
        raise dxpy.DXSearchError(f"Expected one result, but found more: {kwargs}")

    return workflow


def get_app_or_applet(
    app_or_applet_desc: Union[str, dict, dxpy.DXApp, dxpy.DXApplet],
    project: Optional[Union[str, dxpy.DXProject]] = None,
) -> Union[dxpy.DXApp, dxpy.DXApplet]:
    """
    Gets an app(let) by its ID or name. This is a convenience method for resolving an a app(let)
    when, for example, a user can provide either an applet or an app ID/name as a paramter to an
    app.

    Args:
        app_or_applet_desc: ID or name of the app/applet, a dxlink, or an instance of `dxpy.DXApp`
            or `dxpy.DXApplet`.
        project: name, ID, or `dxpy.DXProject` object of project containing applet.

    Returns:
        `dxpy.DXApp  or `dxpy.DXApplet` object.

    Raises:
        * dxpy.DXSearchError if the app(let) does not exist.
        * dxpy.exceptions.PermissionDenied if user does not have proper permissions
            to access the app(let).
    """
    if isinstance(app_or_applet_desc, (dxpy.DXApp, dxpy.DXApplet)):
        return app_or_applet_desc

    if isinstance(app_or_applet_desc, dict):
        return dxpy.get_handler(app_or_applet_desc)

    if app_or_applet_desc.startswith("applet-"):
        app = get_data_object(app_or_applet_desc, project, classname="applet")
        if app is None:
            app = get_app(app_or_applet_desc)
    else:
        app = get_app(app_or_applet_desc)
        if app is None:
            app = get_data_object(app_or_applet_desc, project, classname="applet")

    if app is None:
        raise dxpy.DXSearchError(
            f"Did not find exactly one app(let): {app_or_applet_desc}"
        )

    return app


def get_app(
    app_desc: Union[str, dict, dxpy.DXApp], version: Optional[str] = None, **kwargs
) -> dxpy.DXApp:
    """
    Gets the app with the given ID or name.

    Args:
        app_desc: The app ID or name, or a DXApp instance.
        version: App version.
        kwargs: Additional kwargs to use when looking up the app by name.

    Returns:
        A DXApp object.

    Raises:
        * dxpy.DXSearchError if the app does not exist.
        * dxpy.exceptions.PermissionDenied if user does not have permissions to access the app.
    """
    if isinstance(app_desc, dxpy.DXApp):
        return app_desc

    if isinstance(app_desc, dict):
        return dxpy.get_handler(app_desc)

    if resolver.is_hashid(app_desc):
        try:
            LOG.info("Checking if app with ID %s exists", app_desc)
            executable = dxpy.DXApp(app_desc)
            executable.describe()
            return executable
        except dxpy.DXError:
            pass

    if app_desc.startswith("app-"):
        app_desc = app_desc[4:]

    if "/" in app_desc:
        slash_index = app_desc.index("/")
        if version is None:
            version = app_desc[slash_index + 1 :]
        app_desc = app_desc[:slash_index]

    if version:
        LOG.info("Searching for app by name %s version %s", app_desc, version)
        kwargs.update(dict(all_versions=True, describe=True, return_handler=True))
        for app in dxpy.find_apps(name=app_desc, **kwargs):
            if app.describe()["version"] == version:
                return app
        else:
            raise ValueError(f"no app with name {app_desc} and version {version}")
    else:
        if kwargs.get("all_versions"):
            raise ValueError("cannot specify all_versions=True")
        LOG.info("Searching for app by name %s", app_desc)
        kwargs["return_handler"] = True
        return dxpy.find_one_app(name=app_desc, zero_ok=False, more_ok=False, **kwargs)


def _parse_object_name(name: str) -> Tuple[str, str, bool]:
    """
    Parses a name that might include a path component.

    Args:
        name: The name to parse.

    Returns:
        Tuple (folder, filename, recurse), where `recurse` is whether a search should by default be
        recursive.
    """
    if name.startswith("/"):
        return os.path.split(name) + (False,)
    else:
        return "/", name, True
