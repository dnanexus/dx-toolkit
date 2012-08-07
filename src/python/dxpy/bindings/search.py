'''
There are a few different methods by which existing objects and
entities can be queried.  The :func:`dxpy.bindings.search.find_data_objects`
function will provide search functionality over all data objects
managed by the API server.  All jobs (running, failed, or done) can be
found using :func:`dxpy.bindings.search.find_jobs`.
'''

import dxpy
from dxpy.bindings import *
import time

def now():
    return int(time.time()*1000)

def find_data_objects(classname=None, state=None, visibility=None,
                      name=None, properties=None, typename=None, tag=None,
                      link=None, project=None, folder=None, recurse=None,
                      modified_after=None, modified_before=None,
                      created_after=None, created_before=None,
                      describe=None, limit=None, level=None,
                      return_handler=False, **kwargs):
    """
    :param classname: Class with which to restrict the search, i.e. one of "record", "file", "gtable", "table", "applet"
    :type classname: string
    :param state: State of the object ("open", "closing", "closed", "any")
    :type state: string
    :param visibility: Visibility of the object ("hidden", "visible", "either")
    :type visibility: string
    :param name: Name of the object
    :type name: string
    :param properties: Properties (key-value pairs) that each result must have
    :type properties: dict
    :param typename: Type that each result must conform to
    :type typename: string
    :param tag: Tag that each result must be tagged with
    :type tag: string
    :param link: ID of an object to which each result must link to
    :type link: string
    :param project: ID of a project in which each result must belong
    :type project: string
    :param folder: If *project* is given, full path to a folder in which each result must belong (default is the root folder)
    :type folder: string
    :param recurse: If *project* is given, whether to look in subfolders as well
    :type recurse: boolean
    :param modified_after: Timestamp after which each result was last modified (if negative, interpreted as *modified_after* ms in the past; can be given as string with suffixes)
    :type modified_after: int or string
    :param modified_before: Timestamp before which each result was last modified (if negative, interpreted as *modified_before* ms in the past; can be given as string with suffixes)
    :type modified_before: int or string
    :param created_after: Timestamp after which each result was last created (if negative, interpreted as *created_after* ms in the past; can be given as string with suffixes)
    :type created_after: int or string
    :param created_before: Timestamp before which each result was last created (if negative, interpreted as *created_before* ms in the past; can be given as string with suffixes)
    :type created_before: int or string
    :param describe: Whether to also return the output of calling describe() on the object (if given True) or not (False)
    :type describe: boolean
    :param level: The minimum permissions level for which results should be returned (one of "LIST", "VIEW", "CONTRIBUTE", or "ADMINISTER")
    :type level: string
    :param limit: The maximum number of results to be returned
    :type limit: int
    :param return_handler: Whether to yield results as dxpy object handlers or as a dict with keys "id" and "project"
    :type return_handler: boolean
    :rtype: generator

    This is a generator function which returns the search results and
    handles fetching of future chunks if necessary.  The search is not
    restricted by any fields which are omitted and otherwise imposes
    the restrictions requested.
    
    All timestamps are in milliseconds since the Epoch. Timestamps can also be given as strings with suffixes "s", "m",
    "d", "w", or "y" (for seconds, minutes, days, weeks, or years). The following example finds all items which were
    created more than 1 week ago:
    
        items = list(find_data_objects(created_before="-1w"))

    These two examples iterate through all gtables with property
    "project" set to "cancer project" and prints their object IDs::

        for result in find_data_objects(classname="gtable", properties={"project": "cancer project"}):
            print "Found gtable with object id " + result["id"]

        for result in search(classname="gtable", properties={"project": "cancer project"}, describe=True):
            print "Found gtable with name " + result["describe"]["name"]

    """

    query = {}
    if classname is not None:
        query["class"] = classname
    if state is not None:
        query["state"] = state
    if visibility is not None:
        query["visibility"] = visibility
    if name is not None:
        query["name"] = name
    if properties is not None:
        query["properties"] = properties
    if typename is not None:
        query["type"] = typename
    if tag is not None:
        query["tag"] = tag
    if link is not None:
        query["link"] = link
    if project is not None:
        query["scope"] = {"project": project}
        if folder is not None:
            query["scope"]["folder"] = folder
        if recurse is not None:
            query["scope"]["recurse"] = recurse
    elif folder is not None or recurse is not None:
        if dxpy.WORKSPACE_ID is not None:
            query['scope'] = {'project': dxpy.WORKSPACE_ID}
            if folder is not None:
                query['scope']['folder'] = folder
            if recurse is not None:
                query['scope']['recurse'] = recurse
        else:
            raise DXError("Cannot search within a folder or recurse if a project is not specified")
    if modified_after is not None or modified_before is not None:
        query["modified"] = {}
        if modified_after is not None:
            if isinstance(modified_after, basestring):
                modified_after = dxpy.utils.normalize_timedelta(modified_after)
            if modified_after >= 0:
                query["modified"]["after"] = modified_after
            else:
                query["modified"]["after"] = now() + modified_after
        if modified_before is not None:
            if isinstance(modified_before, basestring):
                modified_before = dxpy.utils.normalize_timedelta(modified_before)
            if modified_before >= 0:
                query["modified"]["before"] = modified_before
            else:
                query["modified"]["before"] = now() + modified_before
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            if isinstance(created_after, basestring):
                created_after = dxpy.utils.normalize_timedelta(created_after)
            if created_after >= 0:
                query["created"]["after"] = created_after
            else:
                query["created"]["after"] = now() + created_after
        if created_before is not None:
            if isinstance(created_before, basestring):
                created_before = dxpy.utils.normalize_timedelta(created_before)
            if created_before >= 0:
                query["created"]["before"] = created_before
            else:
                query["created"]["before"] = now() + created_before
    if describe is not None:
        query["describe"] = describe
    if level is not None:
        query['level'] = level
    if limit is not None:
        query["limit"] = limit

    num_results = 0

    while True:
        resp = dxpy.api.systemFindDataObjects(query, **kwargs)
        
        for i in resp["results"]:
            if num_results == limit:
                raise StopIteration()
            num_results += 1
            if return_handler:
                handler = dxpy.get_handler(i['id'], project=i['project'])
                yield handler
            else:
                yield i

        # set up next query
        if resp["next"] is not None:
            query["starting"] = resp["next"]
        else:
            raise StopIteration()

def find_one_data_object(classname=None, state=None, visibility=None,
                         name=None, properties=None, typename=None, tag=None,
                         link=None, project=None, folder=None, recurse=None,
                         modified_after=None, modified_before=None,
                         created_after=None, created_before=None,
                         describe=None, return_handler=None, **kwargs):
    """
    :param classname: Class with which to restrict the search, i.e. one of "record", "file", "gtable", "table", "applet"
    :type classname: string
    :param state: State of the object ("open", "closing", "closed", "any")
    :type state: string
    :param visibility: Visibility of the object ("hidden", "visible", "either")
    :type visibility: string
    :param name: Name of the object
    :type name: string
    :param properties: Properties (key-value pairs) that each result must have
    :type properties: dict
    :param typename: Type that each result must conform to
    :type typename: string
    :param tag: Tag that each result must be tagged with
    :type tag: string
    :param link: ID of an object to which each result must link to
    :type link: string
    :param project: ID of a project in which each result must belong
    :type project: string
    :param folder: If *project* is given, full path to a folder in which each result must belong (default is the root folder)
    :type folder: string
    :param recurse: If *project* is given, whether to look in subfolders as well
    :type recurse: boolean
    :param modified_after: Timestamp after which each result was last modified (if negative, interpreted as *modified_after* ms in the past)
    :type modified_after: integer
    :param modified_before: Timestamp before which each result was last modified (if negative, interpreted as *modified_before* ms in the past)
    :type modified_before: integer
    :param created_after: Timestamp after which each result was last created (if negative, interpreted as *created_after* ms in the past)
    :type created_after: integer
    :param created_before: Timestamp before which each result was last created (if negative, interpreted as *created_before* ms in the past)
    :type created_before: integer
    :param describe: Whether to also return the output of calling describe() on the object (if given True) or not (False)
    :type describe: boolean
    :param level: The minimum permissions level for which results should be returned (one of "LIST", "VIEW", "CONTRIBUTE", or "ADMINISTER")
    :type level: string
    :param return_handler: Whether to return a result as a dxpy object handler or as a dict with keys "id" and "project"
    :type return_handler: boolean
    :rtype: dict, handler, or None

    This is a function which returns the first data object found which
    satisfies all of the constraints.  If no results are found, it
    returns None.

    """

    query = {}
    if classname is not None:
        query["class"] = classname
    if state is not None:
        query["state"] = state
    if visibility is not None:
        query["visibility"] = visibility
    if name is not None:
        query["name"] = name
    if properties is not None:
        query["properties"] = properties
    if typename is not None:
        query["type"] = typename
    if tag is not None:
        query["tag"] = tag
    if link is not None:
        query["link"] = link
    if project is not None:
        query["scope"] = {"project": project}
        if folder is not None:
            query["scope"]["folder"] = folder
        if recurse is not None:
            query["scope"]["recurse"] = recurse
    elif folder is not None or recurse is not None:
        if dxpy.WORKSPACE_ID is not None:
            query['scope'] = {'project': dxpy.WORKSPACE_ID}
            if folder is not None:
                query['scope']['folder'] = folder
            if recurse is not None:
                query['scope']['recurse'] = recurse
        else:
            raise DXError("Cannot search within a folder or recurse if a project is not specified")
    if modified_after is not None or modified_before is not None:
        query["modified"] = {}
        if modified_after is not None:
            if isinstance(modified_after, basestring):
                modified_after = dxpy.utils.normalize_timedelta(modified_after)
            if modified_after >= 0:
                query["modified"]["after"] = modified_after
            else:
                query["modified"]["after"] = now() + modified_after
        if modified_before is not None:
            if isinstance(modified_before, basestring):
                modified_before = dxpy.utils.normalize_timedelta(modified_before)
            if modified_before >= 0:
                query["modified"]["before"] = modified_before
            else:
                query["modified"]["before"] = now() + modified_before
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            if isinstance(created_after, basestring):
                created_after = dxpy.utils.normalize_timedelta(created_after)
            if created_after >= 0:
                query["created"]["after"] = created_after
            else:
                query["created"]["after"] = now() + created_after
        if created_before is not None:
            if isinstance(created_before, basestring):
                created_before = dxpy.utils.normalize_timedelta(created_before)
            if created_before >= 0:
                query["created"]["before"] = created_before
            else:
                query["created"]["before"] = now() + created_before
    if describe is not None:
        query["describe"] = describe
    query["limit"] = 1

    resp = dxpy.api.systemFindDataObjects(query, **kwargs)

    if len(resp['results']) == 0:
        return None
    else:
        if return_handler:
            handler = dxpy.get_handler(resp['results'][0]['id'],
                                       project=resp['results'][0]['project'])
            return handler
        else:
            return resp['results'][0]

def find_jobs(launched_by=None, applet=None, project=None, state=None,
              origin_job=None, parent_job=None,
              created_after=None, created_before=None, describe=False,
              **kwargs):
    '''
    :param launched_by: User ID of the user who launched the job's origin job
    :type launched_by: string
    :param applet: ID of the applet which spawned this job
    :type applet: string
    :param project: ID of the project context for the job
    :type project: string
    :param state: State of the job (e.g. "failed", "done")
    :type state: string
    :param origin_job: ID of the original job initiated by a user running an applet which eventually spawned this job
    :type origin_job: string
    :param parent_job: ID of the parent job; the string 'none' indicates it should have no parent
    :type parent_job: string
    :param created_after: Timestamp after which each result was last created (if negative, interpreted as *created_after* ms in the past)
    :type created_after: integer
    :param created_before: Timestamp before which each result was last created (if negative, interpreted as *created_before* ms in the past)
    :type created_before: integer
    :param describe: Whether to also return the output of calling describe() on the job (if given True) or not (False) (use the dict {"io": False} to exclude detailed IO information)
    :type describe: boolean or dict
    :rtype: generator

    This is a generator function which returns the search results and
    handles fetching of future chunks if necessary.  The search is not
    restricted by any fields which are omitted and otherwise imposes
    the restrictions requested.

    These two examples iterates through all finished jobs in a
    particular project in the last two days::

        two_days_ago = time.time()
        for result in find_jobs(state="done", project=proj_id,
                                created_after=time.time()-}):
            print "Found gtable with object id " + result["id"]

        for result in search(classname="gtable", properties={"project": "cancer project"}, describe=True):
            print "Found gtable with name " + result["describe"]["name"]

    '''

    query = {}
    if launched_by is not None:
        query["launchedBy"] = launched_by
    if applet is not None:
        if isinstance(applet, DXApplet):
            query["applet"] = applet.get_id()
        else:
            query["applet"] = applet
    if project is not None:
        query["project"] = project
    if state is not None:
        query["state"] = state
    if origin_job is not None:
        query["originJob"] = origin_job
    if parent_job is not None:
        if parent_job == "none":
            query["parentJob"] = None
        else:
            query["parentJob"] = parent_job
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            if isinstance(created_after, basestring):
                created_after = dxpy.utils.normalize_timedelta(created_after)
            if created_after >= 0:
                query["created"]["after"] = created_after
            else:
                query["created"]["after"] = now() + created_after
        if created_before is not None:
            if isinstance(created_before, basestring):
                created_before = dxpy.utils.normalize_timedelta(created_before)
            if created_before >= 0:
                query["created"]["before"] = created_before
            else:
                query["created"]["before"] = now() + created_before
    query["describe"] = describe

    while True:
        resp = dxpy.api.systemFindJobs(query, **kwargs)
        
        for i in resp["results"]:
            yield i

        # set up next query
        if resp["next"] is not None:
            query["starting"] = resp["next"]
        else:
            raise StopIteration()

def find_projects(name=None, level=None, describe=None, **kwargs):
    """
    :param name: Name of the project
    :type name: string
    :param level: Minimum permissions level of returned project IDs
    :type level: string
    :param describe: Either false or the input to the describe call for the project
    :type describe: boolean or dict
    :rtype: generator

    Queries for the user's accessible projects with the specified
    minimum permissions level.

    """
    query = {}
    if name is not None:
        query["name"] = name
    if level is not None:
        query["level"] = level
    if describe is not None:
        query["describe"] = describe

    resp = dxpy.api.systemFindProjects(query, **kwargs)

    for i in resp["results"]:
        yield i

def find_apps(name=None, category=None, all_versions=None, published=None,
              billed_to=None, created_by=None, developer=None,
              created_after=None, created_before=None,
              modified_after=None, modified_before=None,
              describe=None, **kwargs):
    """
    :param name: Name of the app
    :type name: string
    :param category: Name of a category with which to restrict the results
    :type category: string
    :param all_versions: Whether to return all versions of the apps or just the default versions
    :type all_versions: bool
    :param published: Whether to restrict the results to only published apps
    :type published: bool
    :param billed_to: Entity ID (user or organization) that pays for the app's storage costs
    :type billed_to: string
    :param created_by: User ID of the developer that created the version
    :type created_by: string
    :param developer: User ID of a developer of the app
    :type developer: string
    :param created_after: Timestamp after which each result was last created (if negative, interpreted as *created_after* ms in the past)
    :type created_after: integer
    :param created_before: Timestamp before which each result was last created (if negative, interpreted as *created_before* ms in the past)
    :type created_before: integer
    :param modified_after: Timestamp after which each result was last modified (if negative, interpreted as *modified_after* ms in the past)
    :type modified_after: integer
    :param modified_before: Timestamp before which each result was last modified (if negative, interpreted as *modified_before* ms in the past)
    :type modified_before: integer
    :param describe: Whether to also return the output of calling describe() on the object (if given True) or not (False)
    :type describe: boolean
    :rtype: generator
    
    This is a generator function which returns the search results over
    apps and handles fetching of future chunks if necessary.  The
    search is not restricted by any fields which are omitted and
    otherwise imposes the restrictions requested.

    All timestamps are in milliseconds since the Epoch. Timestamps can also be given as strings with suffixes "s", "m",
    "d", "w", or "y" (for seconds, minutes, days, weeks, or years).

    """

    query = {}
    if name is not None:
        query["name"] = name
    if category is not None:
        query["category"] = category
    if all_versions is not None:
        query["allVersions"] = all_versions
    if published is not None:
        query["published"] = published
    if billed_to is not None:
        query["billTo"] = billed_to
    if created_by is not None:
        query["createdBy"] = created_by
    if developer is not None:
        query["developer"] = developer
    if modified_after is not None or modified_before is not None:
        query["modified"] = {}
        if modified_after is not None:
            if isinstance(modified_after, basestring):
                modified_after = dxpy.utils.normalize_timedelta(modified_after)
            if modified_after >= 0:
                query["modified"]["after"] = modified_after
            else:
                query["modified"]["after"] = now() + modified_after
        if modified_before is not None:
            if isinstance(modified_before, basestring):
                modified_before = dxpy.utils.normalize_timedelta(modified_before)
            if modified_before >= 0:
                query["modified"]["before"] = modified_before
            else:
                query["modified"]["before"] = now() + modified_before
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            if created_after >= 0:
                query["created"]["after"] = created_after
            else:
                query["created"]["after"] = now() + created_after
        if created_before is not None:
            if created_before >= 0:
                query["created"]["before"] = created_before
            else:
                query["created"]["before"] = now() + created_before
    if describe is not None:
        query["describe"] = describe

    while True:
        resp = dxpy.api.systemFindApps(query, **kwargs)
        
        for i in resp["results"]:
            yield i

        # set up next query
        if resp["next"] is not None:
            query["starting"] = resp["next"]
        else:
            raise StopIteration()
