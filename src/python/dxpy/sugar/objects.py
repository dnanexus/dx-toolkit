from __future__ import print_function, unicode_literals, division, absolute_import
import os

import dxpy
import dxpy.api
from dxpy.sugar import get_log


LOG = get_log(__name__)


def get_project(id_or_name, level="VIEW", create=False, region=None, **kwargs):
    """Gets a project by name or ID. Creates a new project if there is no existing
    project with the given name and `create is True`.

    Args:
        id_or_name: Project ID (project-XXX) or name. If this is a DXProject object,
            it is validated and returned.
        level: Minimum access level to search.
        create: Whether to create the project if it does not exist.
        region: Region in which to create the project.
        **kwargs: Additional keyword arguments to pass to both
            `find_projects()` and `project_new()`.

    Returns:
        A DXProject object.

    Raises:
        * dxpy.AppError if project does not exist or is not in specified region.
        * dxpy.exceptions.PermissionDenied if user does not have proper permissions
            to access the project.
    """
    id_or_name = id_or_name.strip()

    if isinstance(id_or_name, dxpy.DXProject):
        project = id_or_name
    else:
        # First check if id_or_name is an ID
        try:
            project = dxpy.get_handler(id_or_name)
        except dxpy.DXError:
            project = None

    if project:
        LOG.info("Checking that project with ID %s exists", id_or_name)
        try:
            project_region = project.describe()["region"]
        except dxpy.exceptions.ResourceNotFound:
            LOG.exception("Project with ID %s not found", id_or_name)
            raise dxpy.AppError(
                "Project with ID {} does not exist".format(id_or_name)
            )
        if region and project_region != region:
            raise dxpy.AppError(
                "Project {} region {} does not match expected region {}".format(
                    project.name, project_region, region
                )
            )
        else:
            LOG.info("Project %s exists in region %s", project.name, project_region)
    else:
        LOG.info("Searching for project with name %s", id_or_name)
        find_args = dict(kwargs)
        if region:
            find_args["region"] = region
        if level:
            find_args["level"] = level
        found = list(dxpy.find_projects(id_or_name, **find_args))

        if len(found) == 0 and create:
            LOG.info(
                "Creating project %s in %s region",
                id_or_name, region if region else "default"
            )
            create_args = dict(kwargs)
            create_args["name"] = id_or_name
            if region:
                create_args["region"] = region
            project_id = dxpy.api.project_new(create_args)["id"]
            return dxpy.DXProject(project_id)
        else:
            return _create_one(found, "project", id_or_name)

    return project


def ensure_folder(folder, project=None, exists=None, create=False):
    """Checks that the given project contains (or does not contain) the given folder.
    The folder is created if it does not exist and `create is True`.

    Args:
        folder: The folder to validate.
        project: Project ID, name, or DXProject object. The project in which to look
            for the folder. Defaults to the current project.
        exists: Assert whether the folder exists (True) or does not exist (False).
        create: Whether to create the folder if it does not exist.

    Returns:
        A listing of the objects in the folder.

    Raises:
        dxpy.AppError if the folder does exist and `create is True` or does not exist
            and `create is False`.

    """
    folder = folder.strip()
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
                "Folder %s not found in project %s", folder, project.get_id(),
                exc_info=True
            )
            raise dxpy.AppError(
                "Folder {} does not exist in project {}".format(
                    folder, project.get_id()
                )
            )
        elif create:
            LOG.info("Creating new folder %s in project %s", folder, project.get_id())
            project.new_folder(folder, parents=True)
            return []
    elif exists is False:
        raise dxpy.AppError(
            "Folder {} exists in project {} but was expected not to exist".format(
                folder, project.get_id()
            )
        )
    else:
        return ls


def get_file(id_or_name, project=None, classname="file", **kwargs):
    """Gets a file by name or ID.

    Args:
        id_or_name: ID or name of the file.
        project: The project in which to get the file. Defaults to the current project.
        classname: Classname of the data object; defaults to "file" but can also be
            "record" or "database".
        **kwargs: Additional keyword arguments to use when searching for file.

    Returns:
        A DXFile object.
    """
    if isinstance(id_or_name, dxpy.DXFile):
        return id_or_name

    # First check if id_or_name is an ID
    id_or_name = id_or_name.strip()
    project = get_project(project)
    try:
        return dxpy.get_handler(id_or_name, project)
    except:
        pass

    # Next search by name within the current project
    folder, name, recurse = _parse_name(id_or_name)
    if "folder" not in kwargs:
        kwargs["folder"] = folder
    if "recurse" not in kwargs:
        kwargs["recurse"] = recurse
    return _create_one(
        dxpy.find_data_objects(name=name, classname="file", **kwargs),
        classname,
        id_or_name,
        project
    )


def get_workflow(id_or_name, project=None, **kwargs):
    """
    Search for a project workflow or global workflow with the given ID or name.

    Args:
        id_or_name: A workflow ID (either 'workflow-xxx' or
            'globalworkflow-xxx') or name.
        project: Project ID or DXProject object. The project to search for the
            workflow; if not found here it is expected to be a global workflow.
        kwargs: Additional keyword arguments to use when searching for a workflow
            or globalworkflow by name.

    Returns:
        A DXWorkflow or DXGlobalWorkflow object.

    Raises:
        * dxpy.AppError if workflow does not exist.
        * dxpy.exceptions.PermissionDenied if user does not have proper permissions
            to access the workflow.
    """
    if isinstance(id_or_name, dxpy.DXExecutable):
        return id_or_name

    # First check if id_or_name is an ID
    id_or_name = id_or_name.strip()
    project = get_project(project)
    try:
        return dxpy.get_handler(id_or_name, project.get_id())
    except dxpy.DXError:
        pass

    # The argument must be a name - first search for it in the project
    classname = "workflow"
    workflow_folder, workflow_name, recurse = _parse_name(id_or_name)
    if "folder" not in kwargs:
        kwargs["folder"] = workflow_folder
    if "recurse" not in kwargs:
        kwargs["recurse"] = recurse
    candidates = list(dxpy.find_data_objects(
        classname="workflow",
        name=workflow_name,
        project=project.get_id(),
        **kwargs
    ))

    if not candidates:
        # Finally search for a global workflow
        candidates = list(dxpy.find_global_workflows(
            name=id_or_name,
            **kwargs
        ))
        classname = "globalworkflow"

    return _create_one(candidates, classname, id_or_name, project=project)


def get_app_or_applet(id_or_name, project=None, **kwargs):
    """Gets an app(let) by its ID or name.

    Args:
        id_or_name: ID or name of the app/applet.
        project: name, ID, or DXProject object of project containing applet.
        kwargs: Additional kwargs to use when looking up the app(let) by name.

    Returns:
        DXApp or DXApplet object.

    Raises:
        * dxpy.AppError if the app(let) does not exist.
        * dxpy.exceptions.PermissionDenied if user does not have proper permissions
            to access the app(let).
    """
    if isinstance(id_or_name, dxpy.DXExecutable):
        return id_or_name
    if id_or_name.startswith("app-"):
        return get_app(id_or_name, **kwargs)
    elif id_or_name.startswith("applet-"):
        return get_applet(id_or_name, project, **kwargs)
    else:
        try:
            return get_applet(id_or_name, project, **kwargs)
        except:
            return get_app(id_or_name, **kwargs)


def get_app(id_or_name, **kwargs):
    """Gets the app with the given ID or name.

    Args:
        id_or_name: The app ID or name.
        kwargs: Additional kwargs to use when looking up the app by name.

    Returns:
        A DXApp object.

    Raises:
        * dxpy.AppError if the app does not exist.
        * dxpy.exceptions.PermissionDenied if user does not have proper permissions
            to access the app.
    """
    if isinstance(id_or_name, dxpy.DXApp):
        return id_or_name
    try:
        executable = dxpy.DXApp(id_or_name)
        executable.describe()
        return executable
    except dxpy.exceptions.ResourceNotFound:
        return _create_one(dxpy.find_apps(id_or_name, **kwargs), "app", id_or_name)


def get_applet(id_or_name, project=None, **kwargs):
    """Gets the applet with the given ID or name in the given project.

    Args:
        id_or_name: The applet ID or name.
        project: Project ID or DXProject object. The project in which to look for the
            applet. Defaults to the current project.
        kwargs: Additional kwargs to use when looking up the applet by name.

    Returns:
        A DXApplet object.

    Raises:
        * dxpy.AppError if the applet does not exist.
        * dxpy.exceptions.PermissionDenied if user does not have proper permissions
            to access the applet.
    """
    if isinstance(id_or_name, dxpy.DXApplet):
        return id_or_name
    project = get_project(project)
    try:
        executable = dxpy.DXApplet(id_or_name, project=project.get_id())
        executable.describe()
        return executable
    except:
        pass

    applet_folder, applet_name, recurse = _parse_name(id_or_name)
    if "folder" not in kwargs:
        kwargs["folder"] = applet_folder
    if "recurse" not in kwargs:
        kwargs["recurse"] = recurse
    return _create_one(
        dxpy.find_data_objects(
            "applet",
            name=applet_name,
            project=project.get_id(),
            **kwargs
        ),
        "applet",
        id_or_name,
        project
    )


def _create_one(candidates, classname, id_or_name, project=None):
    """Create handler from list containing exactly one search result.

    Args:
        candidates: List of search results.
        classname: Classname of search results.
        id_or_name: ID or name of object being searched.
        project: Optional project containing the object being searched.

    Returns:
        A subclass of DXObject.

    Raises:
        dxpy.AppError when `candidates` is not of length 1.
    """
    if not candidates:
        raise dxpy.AppError(
            "No {} found with ID or name {}".format(classname, id_or_name)
        )
    elif len(candidates) > 1:
        raise dxpy.AppError(
            "More than one {} found with name {}".format(classname, id_or_name)
        )
    else:
        LOG.info("Found existing %s with name %s", classname, id_or_name)
        return dxpy.get_handler(candidates[0]["id"], project=project)


def _parse_name(name):
    """Parse a name that might include a path component.

    Args:
        name: The name to parse.

    Returns:
        Tuple (folder, filename, recurse)
    """
    if name.startswith("/"):
        return os.path.split(name) + (False,)
    else:
        return "/", name, True
