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
This module provides methods for finding existing objects and entities in the
DNAnexus Platform. The :func:`~dxpy.bindings.search.find_data_objects` function
provides search functionality over all data objects in the system. The
:func:`~dxpy.bindings.search.find_jobs` function can be used to find jobs
(whether they are running, failed, or done).
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
from . import DXApplet, DXApp, DXWorkflow, DXProject, DXJob, DXAnalysis
from ..exceptions import DXError, DXSearchError


def resolve_data_objects(objects, project=None, folder=None, batchsize=1000):
    """
    :param objects: Data object specifications, each with fields "name"
                    (required), "folder", and "project"
    :type objects: list of dictionaries
    :param project: ID of project context; a data object's project defaults
                    to this if not specified for that object
    :type project: string
    :param folder: Folder path within the project; a data object's folder
                   path defaults to this if not specified for that object
    :type folder: string
    :param batchsize: Number of objects to resolve in each batch call to
                      system_resolve_data_objects; defaults to 1000 and is
                      only used for testing (must be a positive integer not
                      exceeding 1000)
    :type batchsize: int
    :returns: List of results parallel to input objects, where each
              entry is a list containing 0 or more dicts, each corresponding
              to a resolved object
    :rtype: List of lists of dictionaries

    Each returned element is a list of dictionaries with keys "project" and
    "id". The number of dictionaries for each element may be 0, 1, or more.
    """
    if not isinstance(batchsize, int) or batchsize <= 0 or batchsize > 1000:
        raise ValueError("batchsize for resolve_data_objects must be a positive integer not exceeding 1000")
    args = {}
    if project:
        args.update({'project': project})
    if folder:
        args.update({'folder': folder})

    results = []

    # Call API method /system/resolveDataObjects in groups of size batchsize
    for i in range(0, len(objects), batchsize):
        args.update({'objects': objects[i:(i+batchsize)]})
        results.extend(dxpy.api.system_resolve_data_objects(args)['results'])
    return results


def _find(api_method, query, limit, return_handler, first_page_size, **kwargs):
    ''' Takes an API method handler (dxpy.api.find*) and calls it with *query*,
    and then wraps a generator around its output. Used by the methods below.

    Note that this function may only be used for /system/find* methods.
    '''
    num_results = 0

    if "limit" not in query:
        query["limit"] = first_page_size

    while True:
        resp = api_method(query, **kwargs)

        by_parent = resp.get('byParent')
        descriptions = resp.get('describe')
        def format_result(result):
            if return_handler:
                result = dxpy.get_handler(result['id'], project=result.get('project'))
            if by_parent is not None:
                return result, by_parent, descriptions
            else:
                return result

        for i in resp["results"]:
            if num_results == limit:
                raise StopIteration()
            num_results += 1
            yield format_result(i)

        # set up next query
        if resp["next"] is not None:
            query["starting"] = resp["next"]
            query["limit"] = min(query["limit"]*2, 1000)
        else:
            raise StopIteration()

def find_data_objects(classname=None, state=None, visibility=None,
                      name=None, name_mode='exact', properties=None,
                      typename=None, tag=None, tags=None,
                      link=None, project=None, folder=None, recurse=None,
                      modified_after=None, modified_before=None,
                      created_after=None, created_before=None,
                      describe=False, limit=None, level=None,
                      return_handler=False, first_page_size=100,
                      **kwargs):
    """
    :param classname:
        Class with which to restrict the search, i.e. one of "record",
        "file", "gtable", "applet", "workflow"
    :type classname: string
    :param state: State of the object ("open", "closing", "closed", "any")
    :type state: string
    :param visibility: Visibility of the object ("hidden", "visible", "either")
    :type visibility: string
    :param name: Name of the object (also see *name_mode*)
    :type name: string
    :param name_mode: Method by which to interpret the *name* field ("exact": exact match, "glob": use "*" and "?" as wildcards, "regexp": interpret as a regular expression)
    :type name_mode: string
    :param properties: Properties (key-value pairs) that each result must have (use value True to require the property key and allow any value)
    :type properties: dict
    :param typename: Type constraint that each result must conform to
    :type typename: string or dict
    :param tag: Tag that each result must be tagged with (deprecated in favor of *tags*)
    :type tag: string
    :param tags: List of tags that each result must have ALL of
    :type tags: list of strings
    :param link: ID of an object that each result must link to
    :type link: string
    :param project: ID of a project in which each result must appear
    :type project: string
    :param folder: If *project* is given, full path to a folder in which each result must belong (default is the root folder)
    :type folder: string
    :param recurse: If *project* is given, whether to look in subfolders of *folder* as well (default is True)
    :type recurse: boolean
    :param modified_after: Timestamp after which each result was last modified (see note below for interpretation)
    :type modified_after: int or string
    :param modified_before: Timestamp before which each result was last modified (see note below for interpretation)
    :type modified_before: int or string
    :param created_after: Timestamp after which each result was last created (see note below for interpretation)
    :type created_after: int or string
    :param created_before: Timestamp before which each result was last created (see note below for interpretation)
    :type created_before: int or string
    :param describe: Controls whether to also return the output of
        calling describe() on each object. Supply False to omit describe
        output, True to obtain the default describe output, or a dict to
        be supplied as the describe call input (which may, among other
        things, be used to customize the set of fields that is returned)
    :type describe: bool or dict
    :param level: The minimum permissions level for which results should be returned (one of "VIEW", "UPLOAD", "CONTRIBUTE", or "ADMINISTER")
    :type level: string
    :param limit: The maximum number of results to be returned (if not specified, the number of results is unlimited)
    :type limit: int
    :param first_page_size: The number of results that the initial API call will return. Subsequent calls will raise this by multiplying by 2 up to a maximum of 1000.
    :type first_page_size: int
    :param return_handler: If True, yields results as dxpy object handlers (otherwise, yields each result as a dict with keys "id" and "project")
    :type return_handler: boolean
    :rtype: generator

    Returns a generator that yields all data objects matching the query,
    up to *limit* objects. It transparently handles paging through the
    result set if necessary. For all parameters that are omitted, the
    search is not restricted by the corresponding field.

    .. note:: All timestamps must be supplied as one of the following:

       * A nonnegative integer, interpreted as milliseconds since the Epoch
       * A negative integer, interpreted as an offset in milliseconds relative
         to the current time
       * A string containing a negative integer with one of the suffixes
         "s", "m", "d", "w", or "y" (for seconds, minutes, days, weeks,
         or years), interpreted as an offset from the current time.

       The following examples both find all items that were created more
       than 1 week ago::

           items1 = list(find_data_objects(created_before="-1w"))
           items2 = list(find_data_objects(created_before=-7*24*60*60*1000))

    This example iterates through all GenomicTables with property
    "project" set to "cancer project" and prints their object IDs::

      for result in find_data_objects(classname="gtable", properties={"project": "cancer project"}):
          print "Found gtable with object id " + result["id"]

    """

    query = {}
    if classname is not None:
        query["class"] = classname
    if state is not None:
        query["state"] = state
    if visibility is not None:
        query["visibility"] = visibility
    if name is not None:
        if name_mode == 'exact':
            query["name"] = name
        elif name_mode == 'glob':
            query['name'] = {'glob': name}
        elif name_mode == 'regexp':
            query['name'] = {'regexp': name}
        else:
            raise DXError('find_data_objects: Unexpected value found for argument name_mode')
    if properties is not None:
        query["properties"] = properties
    if typename is not None:
        query["type"] = typename
    if tag is not None and tags is not None:
        raise DXError('find_data_objects: Arguments "tag" and "tags" cannot both be provided')
    if tag is not None:
        query["tag"] = tag
    if tags is not None:
        query["tags"] = {"$and": tags}
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
            query["modified"]["after"] = dxpy.utils.normalize_time_input(modified_after)
        if modified_before is not None:
            query["modified"]["before"] = dxpy.utils.normalize_time_input(modified_before)
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            query["created"]["after"] = dxpy.utils.normalize_time_input(created_after)
        if created_before is not None:
            query["created"]["before"] = dxpy.utils.normalize_time_input(created_before)
    if describe is not None and describe is not False:
        query["describe"] = describe
    if level is not None:
        query['level'] = level
    if limit is not None:
        query["limit"] = limit

    return _find(dxpy.api.system_find_data_objects, query, limit, return_handler, first_page_size, **kwargs)


def find_executions(classname=None, launched_by=None, executable=None, project=None,
                    state=None, origin_job=None, parent_job=None, no_parent_job=False,
                    parent_analysis=None, no_parent_analysis=False, root_execution=None,
                    created_after=None, created_before=None, describe=False,
                    name=None, name_mode="exact", tags=None, properties=None, limit=None,
                    first_page_size=100, return_handler=False, include_subjobs=True,
                    **kwargs):
    '''
    :param classname:
        Class with which to restrict the search, i.e. one of "job",
        "analysis"
    :type classname: string
    :param launched_by: User ID of the user who launched the execution's origin execution
    :type launched_by: string
    :param executable: ID of the applet or app that spawned this execution, or a corresponding remote object handler
    :type executable: string or a DXApp/DXApplet/DXWorkflow instance
    :param project: ID of the project context for the execution
    :type project: string
    :param state: State of the execution (e.g. "failed", "done")
    :type state: string
    :param origin_job: ID of the original job that eventually spawned this execution (possibly by way of other executions)
    :type origin_job: string
    :param parent_job: ID of the parent job (deprecated: use the string 'none' to indicate it should have no parent job; use *no_parent_job* instead)
    :type parent_job: string
    :param no_parent_job: Indicate results should have no parent job; cannot be set to True with *parent_job* set to a string
    :type no_parent_job: boolean
    :param parent_analysis: ID of the parent analysis (deprecated: use the string 'none' to indicate it should have no parent analysis; use *no_parent_analysis* instead)
    :type parent_analysis: string
    :param no_parent_analysis: Indicate results should have no parent analysis; cannot be set to True with *parent_analysis* set to a string
    :type no_parent_job: boolean
    :param root_execution: ID of the top-level (user-initiated) execution (job or analysis) that eventually spawned this execution (possibly by way of other executions)
    :type root_execution: string
    :param created_after: Timestamp after which each result was last created (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type created_after: int or string
    :param created_before: Timestamp before which each result was last created (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type created_before: int or string
    :param describe: Controls whether to also return the output of
        calling describe() on each execution. Supply False to omit
        describe output, True to obtain the default describe output, or
        a dict to be supplied as the describe call input (which may be
        used to customize the set of fields that is to be returned; for
        example, you can supply {"io": False} to suppress detailed
        information about the execution's inputs and outputs)
    :type describe: bool or dict
    :param name: Name of the job or analysis to search by (also see *name_mode*)
    :type name: string
    :param name_mode: Method by which to interpret the *name* field ("exact": exact match, "glob": use "*" and "?" as wildcards, "regexp": interpret as a regular expression)
    :type name_mode: string
    :param tags: List of tags that each result must have ALL of
    :type tags: list of strings
    :param properties: Properties (key-value pairs) that each result must have (use value True to require the property key and allow any value)
    :type properties: dict
    :param limit: The maximum number of results to be returned (if not specified, the number of results is unlimited)
    :type limit: int
    :param first_page_size: The number of results that the initial API call will return. Subsequent calls will raise this by multiplying by 2 up to a maximum of 1000.
    :type first_page_size: int
    :param return_handler: If True, yields results as dxpy object handlers (otherwise, yields each result as a dict with keys "id" and "project")
    :type return_handler: boolean
    :param include_subjobs: If False, no subjobs will be returned by the API
    :type include_subjobs: boolean
    :rtype: generator

    Returns a generator that yields all executions (jobs or analyses) that match the query. It transparently handles
    paging through the result set if necessary. For all parameters that are omitted, the search is not restricted by
    the corresponding field.

    The following example iterates through all finished jobs and analyses in a
    particular project that were launched in the last two days::

      for result in find_executions(state="done", project=proj_id, created_after="-2d"):
          print "Found job or analysis with object id " + result["id"]

    '''

    query = {}
    if classname is not None:
        query["class"] = classname
    if launched_by is not None:
        query["launchedBy"] = launched_by
    if executable is not None:
        if isinstance(executable, (DXApplet, DXApp, DXWorkflow)):
            query["executable"] = executable.get_id()
        else:
            query["executable"] = executable
    if project is not None:
        if isinstance(project, DXProject):
            query["project"] = project.get_id()
        else:
            query["project"] = project
    if state is not None:
        query["state"] = state
    if origin_job is not None:
        if isinstance(origin_job, DXJob):
            query["originJob"] = origin_job.get_id()
        else:
            query["originJob"] = origin_job
    if parent_job is not None:
        if no_parent_job:
            raise DXError('find_executions: Cannot provide parent_job and set no_parent_job to True')
        if parent_job == "none": # to be deprecated
            query["parentJob"] = None
        elif isinstance(parent_job, DXJob):
            query["parentJob"] = parent_job.get_id()
        else:
            query["parentJob"] = parent_job
    elif no_parent_job:
        query["parentJob"] = None
    if parent_analysis is not None:
        if no_parent_analysis:
            raise DXError('find_executions: Cannot provide parent_analysis and set no_parent_analysis to True')
        if parent_analysis == "none": # to be deprecated
            query["parentAnalysis"] = None
        elif isinstance(parent_analysis, DXAnalysis):
            query["parentAnalysis"] = parent_analysis.get_id()
        else:
            query["parentAnalysis"] = parent_analysis
    elif no_parent_analysis:
        query["parentAnalysis"] = None
    if root_execution is not None:
        if isinstance(root_execution, (DXJob, DXAnalysis)):
            query["rootExecution"] = root_execution.get_id()
        else:
            query["rootExecution"] = root_execution
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            query["created"]["after"] = dxpy.utils.normalize_time_input(created_after)
        if created_before is not None:
            query["created"]["before"] = dxpy.utils.normalize_time_input(created_before)
    if describe is not None and describe is not False:
        query["describe"] = describe
    if name is not None:
        if name_mode == 'exact':
            query['name'] = name
        elif name_mode == 'glob':
            query['name'] = {'glob': name}
        elif name_mode == 'regexp':
            query['name'] = {'regexp': name}
        else:
            raise DXError('find_executions: Unexpected value found for argument name_mode')
    if tags is not None:
        query['tags'] = {'$and': tags}
    if properties is not None:
        query['properties'] = properties
    if include_subjobs is not True:
        query["includeSubjobs"] = include_subjobs
    if limit is not None:
        query["limit"] = limit

    return _find(dxpy.api.system_find_executions, query, limit, return_handler, first_page_size, **kwargs)

def find_jobs(*args, **kwargs):
    """
    This method is identical to :meth:`find_executions()` with the class constraint set to "job".
    """
    kwargs['classname'] = 'job'
    return find_executions(*args, **kwargs)

def find_analyses(*args, **kwargs):
    """
    This method is identical to :meth:`find_executions()` with the class constraint set to "analysis".
    """
    kwargs['classname'] = 'analysis'
    return find_executions(*args, **kwargs)

def find_projects(name=None, name_mode='exact', properties=None, tags=None,
                  level=None, describe=False, explicit_perms=None,
                  public=None, created_after=None, created_before=None, billed_to=None,
                  limit=None, return_handler=False, first_page_size=100, containsPHI=None, **kwargs):
    """
    :param name: Name of the project (also see *name_mode*)
    :type name: string
    :param name_mode: Method by which to interpret the *name* field ("exact": exact match, "glob": use "*" and "?" as wildcards, "regexp": interpret as a regular expression)
    :type name_mode: string
    :param properties: Properties (key-value pairs) that each result must have (use value True to require the property key and allow any value)
    :type properties: dict
    :param tags: Tags that each result must have
    :type tags: list of strings
    :param level: One of "VIEW", "UPLOAD", "CONTRIBUTE", or "ADMINSTER". If specified, only returns projects where the current user has at least the specified permission level.
    :type level: string
    :param describe: Controls whether to also return the output of
        calling describe() on each project. Supply False to omit
        describe output, True to obtain the default describe output, or
        a dict to be supplied as the describe call input (which may be
        used to customize the set of fields that is returned)
    :type describe: bool or dict
    :param explicit_perms: Filter on presence of an explicit permision. If True, matching projects must have an explicit permission (any permission granted directly to the user or an organization to which the user belongs). If False, matching projects must not have any explicit permissions for the user. (default is None, for no filter)
    :type explicit_perms: boolean or None
    :param public: Filter on the project being public. If True, matching projects must be public. If False, matching projects must not be public. (default is None, for no filter)
    :type public: boolean or None
    :param created_after: Timestamp after which each result was created
        (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type created_after: int or string
    :param created_before: Timestamp before which each result was created
        (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type created_before: int or string
    :param billed_to: Entity ID (user or organization) that pays for the project's storage costs
    :type billed_to: string
    :param limit: The maximum number of results to be returned (if not specified, the number of results is unlimited)
    :type limit: int
    :param first_page_size: The number of results that the initial API call will return. Subsequent calls will raise this by multiplying by 2 up to a maximum of 1000.
    :type first_page_size: int
    :param return_handler: If True, yields results as dxpy object handlers (otherwise, yields each result as a dict with keys "id" and "project")
    :type return_handler: boolean
    :param containsPHI: If set to true, only returns projects that contain PHI.
        If set to false, only returns projects that do not contain PHI.
    :type containsPHI: boolean
    :rtype: generator

    Returns a generator that yields all projects that match the query.
    It transparently handles paging through the result set if necessary.
    For all parameters that are omitted, the search is not restricted by
    the corresponding field.

    You can use the *level* parameter to find projects that the user has
    at least a specific level of access to (e.g. "CONTRIBUTE").

    """
    query = {}
    if name is not None:
        if name_mode == 'exact':
            query['name'] = name
        elif name_mode == 'glob':
            query['name'] = {'glob': name}
        elif name_mode == 'regexp':
            query['name'] = {'regexp': name}
        else:
            raise DXError('find_projects: Unexpected value found for argument name_mode')
    if properties is not None:
        query["properties"] = properties
    if tags is not None:
        query["tags"] = {"$and": tags}
    if level is not None:
        query["level"] = level
    if describe is not None and describe is not False:
        query["describe"] = describe
    if explicit_perms is not None:
        query['explicitPermission'] = explicit_perms
    if public is not None:
        query['public'] = public
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            query["created"]["after"] = dxpy.utils.normalize_time_input(created_after)
        if created_before is not None:
            query["created"]["before"] = dxpy.utils.normalize_time_input(created_before)
    if billed_to is not None:
        query['billTo'] = billed_to
    if limit is not None:
        query["limit"] = limit
    if containsPHI is not None:
        query["containsPHI"] = containsPHI

    return _find(dxpy.api.system_find_projects, query, limit, return_handler, first_page_size, **kwargs)

def find_apps(name=None, name_mode='exact', category=None,
              all_versions=None, published=None,
              billed_to=None, created_by=None, developer=None,
              created_after=None, created_before=None,
              modified_after=None, modified_before=None,
              describe=False, limit=None, return_handler=False, first_page_size=100, **kwargs):
    """
    :param name: Name of the app (also see *name_mode*)
    :type name: string
    :param name_mode: Method by which to interpret the *name* field ("exact": exact match, "glob": use "*" and "?" as wildcards, "regexp": interpret as a regular expression)
    :type name_mode: string
    :param category: If specified, only returns apps that are in the specified category
    :type category: string
    :param all_versions: Whether to return all versions of each app or just the version tagged "default"
    :type all_versions: boolean
    :param published: If specified, only returns results that have the specified publish status (True for published apps, False for unpublished apps)
    :type published: boolean
    :param billed_to: Entity ID (user or organization) that pays for the app's storage costs
    :type billed_to: string
    :param created_by: If specified, only returns app versions that were created by the specified user (of the form "user-USERNAME")
    :type created_by: string
    :param developer: If specified, only returns apps for which the specified user (of the form "user-USERNAME") is a developer
    :type developer: string
    :param created_after: Timestamp after which each result was last created (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type created_after: int or string
    :param created_before: Timestamp before which each result was last created (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type created_before: int or string
    :param modified_after: Timestamp after which each result was last modified (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type modified_after: int or string
    :param modified_before: Timestamp before which each result was last modified (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type modified_before: int or string
    :param describe: Controls whether to also return the output of
        calling describe() on each app. Supply False to omit describe
        output, True to obtain the default describe output, or a dict to
        be supplied as the describe call input (which may be used to
        customize the set of fields that is returned)
    :type describe: bool or dict
    :param limit: The maximum number of results to be returned (if not specified, the number of results is unlimited)
    :type limit: int
    :param first_page_size: The number of results that the initial API call will return. Subsequent calls will raise this by multiplying by 2 up to a maximum of 1000.
    :type first_page_size: int
    :param return_handler: If True, yields results as dxpy object handlers (otherwise, yields each result as a dict with keys "id" and "project")
    :type return_handler: boolean
    :rtype: generator

    Returns a generator that yields all apps that match the query. It
    transparently handles paging through the result set if necessary.
    For all parameters that are omitted, the search is not restricted by
    the corresponding field.

    """

    query = {}
    if name is not None:
        if name_mode == 'exact':
            query['name'] = name
        elif name_mode == 'glob':
            query['name'] = {'glob': name}
        elif name_mode == 'regexp':
            query['name'] = {'regexp': name}
        else:
            raise DXError('find_apps: Unexpected value found for argument name_mode')
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
            query["modified"]["after"] = dxpy.utils.normalize_time_input(modified_after)
        if modified_before is not None:
            query["modified"]["before"] = dxpy.utils.normalize_time_input(modified_before)
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            query["created"]["after"] = dxpy.utils.normalize_time_input(created_after)
        if created_before is not None:
            query["created"]["before"] = dxpy.utils.normalize_time_input(created_before)
    if describe is not None and describe is not False:
        query["describe"] = describe
    if limit is not None:
        query["limit"] = limit

    return _find(dxpy.api.system_find_apps, query, limit, return_handler, first_page_size, **kwargs)

def _find_one(method, zero_ok=False, more_ok=True, **kwargs):
    kwargs["limit"] = 1 if more_ok else 2
    response = method(**kwargs)
    try:
        result = next(response)
        if not more_ok:
            try:
                next(response)
                raise DXSearchError("Expected one result, but found more: "+str(kwargs))
            except StopIteration:
                pass
    except StopIteration:
        if zero_ok:
            return None
        else:
            raise DXSearchError("Expected one result, but found none: "+str(kwargs))
    return result


def find_one_data_object(zero_ok=False, more_ok=True, **kwargs):
    """
    :param zero_ok:
        If False (default), :class:`~dxpy.exceptions.DXSearchError` is
        raised if the search has 0 results; if True, returns None if the
        search has 0 results
    :type zero_ok: bool
    :param more_ok:
        If False, :class:`~dxpy.exceptions.DXSearchError` is raised if
        the search has 2 or more results
    :type more_ok: bool

    Returns one data object that satisfies the supplied constraints, or
    None if none exist (provided *zero_ok* is True). Supports all search
    constraint arguments supported by :meth:`find_data_objects()`.

    """
    return _find_one(find_data_objects, zero_ok=zero_ok, more_ok=more_ok, **kwargs)


def find_one_project(zero_ok=False, more_ok=True, **kwargs):
    """
    :param zero_ok:
        If False (default), :class:`~dxpy.exceptions.DXSearchError` is
        raised if the search has 0 results; if True, returns None if the
        search has 0 results
    :type zero_ok: bool
    :param more_ok:
        If False, :class:`~dxpy.exceptions.DXSearchError` is raised if
        the search has 2 or more results
    :type more_ok: bool

    Returns one project that satisfies the supplied constraints, or None
    if none exist (provided *zero_ok* is True). Supports all search
    constraint arguments supported by :meth:`find_projects()`.

    """
    return _find_one(find_projects, zero_ok=zero_ok, more_ok=more_ok, **kwargs)


def find_one_app(zero_ok=False, more_ok=True, **kwargs):
    """
    :param zero_ok:
        If False (default), :class:`~dxpy.exceptions.DXSearchError` is
        raised if the search has 0 results; if True, returns None if the
        search has 0 results
    :type zero_ok: bool
    :param more_ok:
        If False, :class:`~dxpy.exceptions.DXSearchError` is raised if
        the search has 2 or more results
    :type more_ok: bool

    Returns one app that satisfies the supplied constraints, or None if
    none exist (provided *zero_ok* is True). Supports all search
    constraint arguments supported by :meth:`find_apps()`.

    """
    return _find_one(find_apps, zero_ok=zero_ok, more_ok=more_ok, **kwargs)


def _org_find(api_method, org_id, query, first_page_size=100):
    """
    Takes an API method handler ``dxpy.api.org_find...`` and calls it with
    *org_id* and *query*, then wraps a generator around its output. Used by
    :meth:`org_find_members` and :meth:`org_find_projects` below.

    :param first_page_size: The number of results that the initial API call will return.
    :type first_page_size: int

    """
    if "limit" not in query:
        query["limit"] = min(first_page_size, 1000)

    while True:
        resp = api_method(org_id, query)
        for result in resp["results"]:
            yield result

        # set up next query
        if resp["next"] is not None:
            query["starting"] = resp["next"]
            query["limit"] = min(query["limit"] * 2, 1000)
        else:
            break


def org_find_members(org_id=None, level=None, describe=False):
    """
    :param org_id: ID of the organization
    :type org_id: string
    :param level: The membership level in the org that each member in the result set must have (one of "MEMBER" or
        "ADMIN")
    :type level: string
    :param describe: Whether or not to return the response of ``dxpy.api.user_describe`` for each result. False omits
        the describe response; True includes it; a dict will be used as the input to ``dxpy.api.user_describe`` (to
        customize the desired set of fields in the describe response).
    :type describe: bool or dict

    Returns a generator that yields all org members that match the query formed by intersecting all specified
    constraints. The search is not restricted by any parameters that were unspecified.
    """
    query = {}
    if level is not None:
        query["level"] = level
    query["describe"] = describe

    return _org_find(dxpy.api.org_find_members, org_id, query)


def org_find_projects(org_id=None, name=None, name_mode='exact', ids=None, properties=None, tags=None, describe=False,
                      public=None, created_after=None, created_before=None, containsPHI=None):
    """
    :param org_id: ID of the organization
    :type org_id: string
    :param name: Name that each result must have (also see *name_mode* param)
    :type name: string
    :param name_mode: Method by which to interpret the *name* param ("exact": exact match,
        "glob": use "*" and "?" as wildcards, "regexp": interpret as a regular expression)
    :type name_mode: string
    :param ids: List of project IDs. Each result must have a project ID that was specified in this list.
    :type ids: array of strings
    :param properties: Properties (key-value pairs) that each result must have
        (use value True to require the property key and allow any value)
    :type properties: dict
    :param tags: Tags that each result must have
    :type tags: list of strings
    :param describe: Whether or not to return the response of ``dxpy.api.project_describe`` for each result. False
        omits the describe response; True includes it; a dict will be used as the input to
        ``dxpy.api.project_describe`` (to customize the desired set of fields in the describe response).
    :type describe: bool or dict
    :param public: True indicates that each result must be public; False indicates that each result must be private;
        None indicates that both public and private projects will be returned in the result set.
    :type public: boolean or None
    :param created_after: Timestamp after which each result was created
        (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type created_after: int or string
    :param created_before: Timestamp before which each result was created
        (see note accompanying :meth:`find_data_objects()` for interpretation)
    :type created_before: int or string
    :param containsPHI: If set to true, only returns projects that contain PHI.
        If set to false, only returns projects that do not contain PHI.
    :type containsPHI: boolean
    :rtype: generator

    Returns a generator that yields all projects that match the query formed by intersecting all specified
    constraints. The search is not restricted by any parameters that were unspecified.

    """
    query = {}
    if name is not None:
        if name_mode == 'exact':
            query['name'] = name
        elif name_mode == 'glob':
            query['name'] = {'glob': name}
        elif name_mode == 'regexp':
            query['name'] = {'regexp': name}
        else:
            raise DXError('org_find_projects: Unexpected value found for argument name_mode')
    if ids is not None:
        query["id"] = ids
    if properties is not None:
        if len(properties.keys()) == 1:
            query["properties"] = properties
        else:
            query["properties"] = {"$and": [{k: v} for (k, v) in properties.iteritems()]}
    if tags is not None:
        if len(tags) == 1:
            query["tags"] = tags[0]
        else:
            query["tags"] = {"$and": tags}
    query["describe"] = describe
    if public is not None:
        query['public'] = public
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            query["created"]["after"] = dxpy.utils.normalize_time_input(created_after)
        if created_before is not None:
            query["created"]["before"] = dxpy.utils.normalize_time_input(created_before)
    if containsPHI is not None:
        query["containsPHI"] = containsPHI

    return _org_find(dxpy.api.org_find_projects, org_id, query)


def org_find_apps(org_id,
                  name=None,
                  name_mode='exact',
                  category=None,
                  all_versions=None,
                  published=None,
                  created_by=None,
                  developer=None,
                  authorized_user=None,
                  created_after=None,
                  created_before=None,
                  modified_after=None,
                  modified_before=None,
                  describe=False,
                  limit=None,
                  return_handler=False,
                  first_page_size=100,
                  **kwargs):
    """
    :param name: Name of the app (also see *name_mode*)
    :type name: string
    :param name_mode: Method by which to interpret the *name* field
        "exact": exact match,
        "glob": use "*" and "?" as wildcards,
        "regexp": interpret as a regular expression
    :type name_mode: string
    :param category: If specified, only returns apps that are in the specified category
    :type category: string
    :param all_versions: Whether to return all versions of each app or just the version tagged "default"
    :type all_versions: boolean
    :param published: If specified, only returns results that have the specified publish status
        True for published apps,
        False for unpublished apps
    :type published: boolean
    :param created_by: If specified, only returns app versions that were created by the specified user
        (of the form "user-USERNAME")
    :type created_by: string
    :param developer: If specified, only returns apps for which the specified user (of the form "user-USERNAME")
        is a developer
    :type developer: string
    :param authorized_user: If specified, only returns apps for which the specified user (either a user ID, org ID,
        or the string "PUBLIC") appears in the app's list of authorized users
    :type authorized_user: string
    :param created_after: Timestamp after which each result was last created (see note accompanying
        :meth:`find_data_objects()` for interpretation)
    :type created_after: int or string
    :param created_before: Timestamp before which each result was last created (see note accompanying
        :meth:`find_data_objects()` for interpretation)
    :type created_before: int or string
    :param modified_after: Timestamp after which each result was last modified (see note accompanying
        :meth:`find_data_objects()` for interpretation)
    :type modified_after: int or string
    :param modified_before: Timestamp before which each result was last modified (see note accompanying
        :meth:`find_data_objects()` for interpretation)
    :type modified_before: int or string
    :param describe: Controls whether to also return the output of
        calling describe() on each app. Supply False to omit describe
        output, True to obtain the default describe output, or a dict to
        be supplied as the describe call input (which may be used to
        customize the set of fields that is returned)
    :type describe: bool or dict
    :param limit: The maximum number of results to be returned (if not specified, the number of results is unlimited)
    :type limit: int
    :param first_page_size: The number of results that the initial API call will return. Subsequent calls will raise
        this by multiplying by 2 up to a maximum of 1000.
    :type first_page_size: int
    :param return_handler: If True, yields results as dxpy object handlers (otherwise, yields each result as a dict
        with keys "id" and "project")
    :type return_handler: boolean
    :rtype: generator

    Returns a generator that yields all apps that match the query. It
    transparently handles paging through the result set if necessary.
    For all parameters that are omitted, the search is not restricted by
    the corresponding field.

    """

    query = {}
    if name is not None:
        if name_mode == 'exact':
            query['name'] = name
        elif name_mode == 'glob':
            query['name'] = {'glob': name}
        elif name_mode == 'regexp':
            query['name'] = {'regexp': name}
        else:
            raise DXError('find_apps: Unexpected value found for argument name_mode')
    if category is not None:
        query["category"] = category
    if all_versions is not None:
        query["allVersions"] = all_versions
    if published is not None:
        query["published"] = published
    if created_by is not None:
        query["createdBy"] = created_by
    if developer is not None:
        query["developer"] = developer
    if authorized_user is not None:
        query["authorizedUser"] = authorized_user
    if modified_after is not None or modified_before is not None:
        query["modified"] = {}
        if modified_after is not None:
            query["modified"]["after"] = dxpy.utils.normalize_time_input(modified_after)
        if modified_before is not None:
            query["modified"]["before"] = dxpy.utils.normalize_time_input(modified_before)
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            query["created"]["after"] = dxpy.utils.normalize_time_input(created_after)
        if created_before is not None:
            query["created"]["before"] = dxpy.utils.normalize_time_input(created_before)
    if describe is not None and describe is not False:
        query["describe"] = describe
    if limit is not None:
        query["limit"] = limit

    return _org_find(dxpy.api.org_find_apps, org_id, query)

def find_orgs(query, first_page_size=10):
    """
    :param query: The input to the /system/findOrgs API method.
    :type query: dict

    :param first_page_size: The number of results that the initial
        /system/findOrgs API call will return; default 10, max 1000. Subsequent
        calls will raise the number of returned results exponentially up to a
        max of 1000.
    :type first_page_size: int

    :rtype: generator

    Returns a generator that yields all orgs matching the specified query. Will
    transparently handle pagination as necessary.
    """
    return _find(dxpy.api.system_find_orgs, query, limit=None,
                 return_handler=False, first_page_size=first_page_size)
