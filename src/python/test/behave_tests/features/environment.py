from helpers.EnvironmentHelper import *
from helpers.DXPYhelper import *
from helpers.AppHelper import *
from dxpy.exceptions import ResourceNotFound, InvalidState

platform = get_platform_from_env()


def before_scenario(context, scenario):
    if EnvironmentHelper.skip_tags(scenario):
        scenario.mark_skipped()
        return

    # List of dictionaries of projects with values for: Name, Id
    context.projects = []
    # Dictionary of dictionaries of files with values for: file, name, size
    context.files = Dotdict({})
    context.platform = platform
    context.dxpy_helper = DXPYhelper(platform)


def after_scenario(context, scenario):
    # Making sure the administer user is logged in
    context.dxpy_helper = DXPYhelper(platform)

    # if failed - dont delete else delete
    if not scenario.status == "failed":
        if hasattr(context, 'orgs'):
            context.dxpy_helper.delete_all_projects(context.orgs)

    if hasattr(context, "projects") and (not hasattr(context, "keep_project") or not context.keep_project):
        context.dxpy_helper.delete_all_projects_non_orgs(context.projects)

    if hasattr(context, "app_id"):
        try:
            delete_app(context.app_id)
        except (ResourceNotFound, InvalidState):
            # If the project with the applet was already deleted or the app was already deleted, ignore.
            pass

    if hasattr(context, "generated_app_name"):
        cleanup_app_dir(context.generated_app_name)
