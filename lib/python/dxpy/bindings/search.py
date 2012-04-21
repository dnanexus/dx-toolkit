'''
There are a few different methods by which existing objects and
entities can be queried.  The :func:`dxpy.bindings.search.find_data_objects`
function will provide search functionality over all data objects
managed by the API server.  All jobs (running, failed, or done) can be
found using :func:`dxpy.bindings.search.find_jobs`.
'''

def find_data_objects(classname=None, state=None, visibility=None,
                      name=None, properties=None, type_=None, tag=None,
                      link=None, project=None, folder=None, recurse=None,
                      modified_after=None, modified_before=None,
                      created_after=None, created_before=None,
                      describe=False):
    """
    :param classname: Class with which to restrict the search, i.e. one of "record", "file", "gtable", "table", "program"
    :type classname: string
    :param state: State of the object ("open", "closing", "closed", "any")
    :type state: string
    :param name: Name of the object
    :type name: string
    :param properties: Properties (key-value pairs) that each result must have
    :type properties: dict
    :param type_: Type that each result must conform to
    :type type_: string
    :param tag: Tag that each result must conform to
    :type tag: string
    :param link: ID of an object to which each result must link to
    :type link: string
    :param project: ID of a project in which each result must belong
    :type project: string
    :param folder: If *project* is given, full path to a folder in which each result must belong (default is the root folder)
    :type folder: string
    :param recurse: If *project* is given, whether to look in subfolders as well
    :type recurse: boolean
    :param modified_after: Timestamp after which each result was last modified
    :type modified_after: integer
    :param modified_before: Timestamp before which each result was last modified
    :type modified_before: integer
    :param created_after: Timestamp after which each result was last created
    :type created_after: integer
    :param created_before: Timestamp before which each result was last created
    :type created_before: integer
    :param describe: Whether to also return the output of calling describe() on the object (if given True) or not (False)
    :type describe: boolean
    :rtype: generator

    This is a generator function which returns the search results and
    handles fetching of future chunks if necessary.  The search is not
    restricted by any fields which are omitted and otherwise imposes
    the restrictions requested.  All timestamps are in milliseconds
    since the Epoch.

    These two examples iterates through all gtables with property
    "project" set to "cancer project" and prints their object IDs::

        for result in find_data_objects(classname="gtable", properties={"project": "cancer project"}):
            print "Found gtable with object id " + result["objectId"]

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
    if type_ is not None:
        query["type"] = type_
    if tag is not None:
        query["tag"] = tag
    if link is not None:
        query["link"] = link
    if project is not None:
        query["scope"]["project"] = project
        if folder is not None:
            query["scope"]["folder"] = folder
        if recurse is not None:
            query["scope"]["recurse"] = recurse
    if modified_after is not None or modified_before is not None:
        query["modified"] = {}
        if modified_after is not None:
            query["modified"]["after"] = modified_after
        if modified_before is not None:
            query["modified"]["before"] = modified_before
    if created_after is not None or created_before is not None:
        query["created"] = {}
        if created_after is not None:
            query["created"]["after"] = created_after
        if created_before is not None:
            query["created"]["before"] = created_before
    query["describe"] = describe

    while True:
        resp = dxpy.api.systemFindDataObjects(query)
        
        for i in resp["results"]:
            yield i

        # set up next query
        if resp["next"] is not None:
            query["starting"] = resp["next"]
        else:
            raise StopIteration()

def find_jobs(launched_by=None, program=None, project=None, state=None,
              origin_job=None, parent_job=None,
              modified_after=None, modified_before=None, describe=False):
    '''
    :param launched_by: User ID of the user who launched the job's origin job
    :type launched_by: string
    :param program: ID of the program which spawned this job
    :type program: string
    :param project: ID of the project context for the job
    :type project: string
    :param state: State of the job (e.g. "failed", "done")
    :type state: string
    :param origin_job: ID of the original job initiated by a user running a program which eventually spawned this job
    :type origin_job: string
    :param parent_job: ID of the parent job
    :type parent_job: string
    :param modified_after: Timestamp after which each result was last modified
    :type modified_after: integer
    :param modified_before: Timestamp before which each result was last modified
    :type modified_before: integer
    :param describe: Whether to also return the output of calling describe() on the job (if given True) or not (False)
    :type describe: boolean
    :rtype: generator

    This is a generator function which returns the search results and
    handles fetching of future chunks if necessary.  The search is not
    restricted by any fields which are omitted and otherwise imposes
    the restrictions requested.

    These two examples iterates through all finished jobs in a
    particular project in the last two days::

        two_days_ago = time.time()
        for result in find_jobs(state="done", project=proj_id,
                                modified_after=time.time()-}):
            print "Found gtable with object id " + result["objectId"]

        for result in search(classname="gtable", properties={"project": "cancer project"}, describe=True):
            print "Found gtable with name " + result["describe"]["name"]

    '''

    query = {}
    if launched_by is not None:
        query["launchedBy"] = launched_by
    if program is not None:
        query["program"] = program
    if project is not None:
        query["project"] = project
    if state is not None:
        query["state"] = state
    if origin_job is not None:
        query["originJob"] = origin_job
    if parent_job is not None:
        query["parentJob"] = parent_job
    if modified_after is not None or modified_before is not None:
        query["modified"] = {}
        if modified_after is not None:
            query["modified"]["after"] = modified_after
        if modified_before is not None:
            query["modified"]["before"] = modified_before
    query["describe"] = describe

    while True:
        resp = dxpy.api.systemFindJobs(query)
        
        for i in resp["results"]:
            yield i

        # set up next query
        if resp["next"] is not None:
            query["starting"] = resp["next"]
        else:
            raise StopIteration()
