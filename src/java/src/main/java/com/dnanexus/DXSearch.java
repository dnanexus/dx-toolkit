// Copyright (C) 2013 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

package com.dnanexus;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Utility class containing methods for searching for platform objects by various criteria.
 */
public final class DXSearch {

    @JsonInclude(Include.NON_NULL)
    private static class FindDataObjectsRequest {

        private static class ExactNameQuery implements NameQuery {
            private final String nameExact;

            private ExactNameQuery(String nameExact) {
                this.nameExact = nameExact;
            }

            @SuppressWarnings("unused")
            @JsonValue
            private Object getValue() {
                return this.nameExact;
            }
        }

        private static class GlobNameQuery implements NameQuery {
            private final String glob;

            private GlobNameQuery(String glob) {
                this.glob = glob;
            }

            @SuppressWarnings("unused")
            @JsonValue
            private Map<String, String> getValue() {
                return ImmutableMap.of("glob", this.glob);
            }
        }

        // The following polymorphic classes, and this interface, are for
        // generating the values that can appear in the "name" field of the
        // query.
        @JsonInclude(Include.NON_NULL)
        private static interface NameQuery {
            // Subclasses below choose what fields to put in their JSON representations.
        }

        private static class RegexpNameQuery implements NameQuery {
            private final String regexp;
            private final String flags;

            private RegexpNameQuery(String regexp) {
                this.regexp = regexp;
                this.flags = null;
            }

            private RegexpNameQuery(String regexp, String flags) {
                this.regexp = regexp;
                this.flags = flags;
            }

            @SuppressWarnings("unused")
            @JsonValue
            private Map<String, String> getValue() {
                ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
                mapBuilder.put("regexp", this.regexp);
                if (this.flags != null) {
                    mapBuilder.put("flags", this.flags);
                }
                return mapBuilder.build();
            }
        }

        @JsonInclude(Include.NON_NULL)
        private static class ScopeQuery {
            @SuppressWarnings("unused")
            @JsonProperty
            private final String project;
            @SuppressWarnings("unused")
            @JsonProperty
            private final String folder;
            @SuppressWarnings("unused")
            @JsonProperty
            private final Boolean recurse;

            private ScopeQuery(String projectId) {
                this(projectId, null, null);
            }

            private ScopeQuery(String projectId, String folder) {
                this(projectId, folder, null);
            }

            private ScopeQuery(String projectId, String folder, Boolean recurse) {
                this.project = projectId;
                this.folder = folder;
                this.recurse = recurse;
            }
        }

        @SuppressWarnings("unused")
        @JsonProperty
        private final NameQuery name;
        @SuppressWarnings("unused")
        @JsonProperty
        private final ScopeQuery scope;
        @SuppressWarnings("unused")
        @JsonProperty("class")
        private final String classConstraint;

        @SuppressWarnings("unused")
        @JsonProperty
        private final FindDataObjectsResponse.Entry starting;
        @SuppressWarnings("unused")
        @JsonProperty
        private final Integer limit;

        /**
         * Creates a new {@code FindDataObjectsRequest} that clones the specified request, but
         * changes the starting value and limit.
         *
         * @param previousQuery previous query to clone
         * @param next starting value for subsequent results
         * @param limit maximum number of results to return, or null to use the default
         *        (server-provided) limit
         */
        private FindDataObjectsRequest(FindDataObjectsRequest previousQuery,
                FindDataObjectsResponse.Entry next, Integer limit) {
            this.name = previousQuery.name;
            this.scope = previousQuery.scope;
            this.classConstraint = previousQuery.classConstraint;

            this.starting = next;
            this.limit = limit;
        }

        /**
         * Creates a new {@code FindDataObjectsRequest} from the query parameters set in the
         * specified builder.
         *
         * @param builder builder object to initialize this query with
         */
        private FindDataObjectsRequest(FindDataObjectsRequestBuilder<?> builder) {
            this.name = builder.nameQuery;
            this.scope = builder.scopeQuery;
            this.classConstraint = builder.classConstraint;

            this.starting = null;
            this.limit = null;
        }

    }

    /**
     * Builder class for formulating {@code findDataObjects} queries and executing them.
     *
     * <p>
     * Obtain an instance of this class via {@link #findDataObjects()}.
     * </p>
     *
     * @param <T> data object class to be returned from the query
     */
    public static class FindDataObjectsRequestBuilder<T extends DXDataObject> {
        private String classConstraint;
        private FindDataObjectsRequest.NameQuery nameQuery;
        private FindDataObjectsRequest.ScopeQuery scopeQuery;

        private final DXEnvironment env;

        private FindDataObjectsRequestBuilder() {
            this.env = DXEnvironment.create();
        }

        private FindDataObjectsRequestBuilder(DXEnvironment env) {
            this.env = env;
        }

        @VisibleForTesting
        FindDataObjectsRequest buildRequestHash() {
            // Use this method to test the JSON hash created by a particular
            // builder call without actually executing the request.
            return new FindDataObjectsRequest(this);
        }

        /**
         * Executes the query.
         *
         * @return object encapsulating the result set
         */
        public FindDataObjectsResult<T> execute() {
            return new FindDataObjectsResult<T>(this.buildRequestHash(), this.classConstraint,
                    this.env);
        }

        /**
         * Executes the query with the specified page size.
         *
         * @param pageSize number of results to obtain on each request
         *
         * @return object encapsulating the result set
         */
        public FindDataObjectsResult<T> execute(int pageSize) {
            return new FindDataObjectsResult<T>(this.buildRequestHash(), this.classConstraint,
                    this.env, pageSize);
        }

        /**
         * Only returns objects in the specified folder of the specified project (not in
         * subfolders).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #inFolderOrSubfolders(DXContainer, String)} and
         * {@link #inProject(DXContainer)}.
         * </p>
         *
         * @param project project or container to search in
         * @param folder full path to folder to search in (a String starting with {@code "/"})
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> inFolder(DXContainer project, String folder) {
            Preconditions.checkArgument(this.scopeQuery == null,
                    "Cannot specify inProject, inFolder, or inFolderOrSubfolders more than once");
            Preconditions.checkNotNull(project);
            Preconditions.checkNotNull(folder);
            this.scopeQuery = new FindDataObjectsRequest.ScopeQuery(project.getId(), folder);
            return this;
        }

        /**
         * Only returns objects in the specified folder of the specified project or in its
         * subfolders.
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #inFolder(DXContainer, String)} and {@link #inProject(DXContainer)}
         * .
         * </p>
         *
         * @param project project or container to search in
         * @param folder full path to folder to search in (a String starting with {@code "/"})
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> inFolderOrSubfolders(DXContainer project,
                String folder) {
            Preconditions.checkArgument(this.scopeQuery == null,
                    "Cannot specify inProject, inFolder, or inFolderOrSubfolders more than once");
            Preconditions.checkNotNull(project);
            Preconditions.checkNotNull(folder);
            this.scopeQuery = new FindDataObjectsRequest.ScopeQuery(project.getId(), folder, true);
            return this;
        }

        /**
         * Only returns objects in the specified project.
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #inFolder(DXContainer, String)} and
         * {@link #inFolderOrSubfolders(DXContainer, String)}.
         * </p>
         *
         * @param project project or container to search in
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> inProject(DXContainer project) {
            Preconditions.checkArgument(this.scopeQuery == null,
                    "Cannot specify inProject, inFolder, or inFolderOrSubfolders more than once");
            Preconditions.checkNotNull(project);
            this.scopeQuery = new FindDataObjectsRequest.ScopeQuery(project.getId());
            return this;
        }

        /**
         * Only returns objects whose names exactly equal the specified string.
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #nameMatchesGlob(String)}, {@link #nameMatchesRegexp(String)}, and
         * {@link #nameMatchesRegexp(String, boolean)}.
         * </p>
         *
         * @param name basename of data object
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> nameMatchesExactly(String name) {
            Preconditions.checkArgument(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            Preconditions.checkNotNull(name);
            this.nameQuery = new FindDataObjectsRequest.ExactNameQuery(name);
            return this;
        }

        /**
         * Only returns objects whose name match the specified glob.
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #nameMatchesExactly(String)}, {@link #nameMatchesRegexp(String)},
         * and {@link #nameMatchesRegexp(String, boolean)}.
         * </p>
         *
         * @param glob shell-like pattern to be matched against data object basename
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> nameMatchesGlob(String glob) {
            Preconditions.checkArgument(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            Preconditions.checkNotNull(glob);
            this.nameQuery = new FindDataObjectsRequest.GlobNameQuery(glob);
            return this;
        }

        /**
         * Only returns objects whose names match the specified regexp.
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #nameMatchesExactly(String)}, {@link #nameMatchesGlob(String)}, and
         * {@link #nameMatchesRegexp(String, boolean)}.
         * </p>
         *
         * @param regexp regexp to be matched against data object basename
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> nameMatchesRegexp(String regexp) {
            Preconditions.checkArgument(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            Preconditions.checkNotNull(regexp);
            this.nameQuery = new FindDataObjectsRequest.RegexpNameQuery(regexp);
            return this;
        }

        /**
         * Only returns objects whose names match the specified regexp (optionally allowing the
         * match to be case insensitive).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #nameMatchesExactly(String)}, {@link #nameMatchesGlob(String)}, and
         * {@link #nameMatchesRegexp(String)}.
         * </p>
         *
         * @param regexp regexp to be matched against data object basename
         * @param caseInsensitive if true, the regexp is matched case-insensitively
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> nameMatchesRegexp(String regexp,
                boolean caseInsensitive) {
            Preconditions.checkArgument(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            Preconditions.checkNotNull(regexp);
            this.nameQuery =
                    new FindDataObjectsRequest.RegexpNameQuery(regexp, caseInsensitive ? "i" : null);
            return this;
        }

        /**
         * Only returns records (filters out data objects of all other classes).
         *
         * <p>
         * This method may only be called once during the construction of a query.
         * </p>
         *
         * @return the same builder object
         */
        @SuppressWarnings("unchecked")
        public FindDataObjectsRequestBuilder<DXRecord> ofClassRecord() {
            Preconditions.checkArgument(this.classConstraint == null,
                    "Cannot specify class constraints more than once");
            this.classConstraint = "record";
            // This cast should be safe, since we hold no references of type T
            return (FindDataObjectsRequestBuilder<DXRecord>) this;
        }

    }

    /**
     * Deserialized output from the system/findDataObjects route.
     */
    @VisibleForTesting
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class FindDataObjectsResponse {

        private static class Entry {
            @JsonProperty
            private String id;
            @JsonProperty
            private String project;
        }

        @JsonProperty
        private List<Entry> results;

        @SuppressWarnings("unused")
        @JsonProperty
        private Entry next;

    }

    /**
     * The set of jobs that matched a {@code findDataObjects} query.
     *
     * <p>
     * This class paginates through the results as necessary to return the full result set.
     * </p>
     *
     * @param <T> data object class to be returned
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FindDataObjectsResult<T extends DXDataObject> implements ObjectProducer<T> {

        // TODO: lazily load results and provide an iterator in addition to
        // buffered List access

        private final FindDataObjectsRequest baseQuery;
        private final String classConstraint;
        private final DXEnvironment env;

        // Number of results to fetch with each API call, or null to use the default
        private final Integer pageSize;

        /**
         * Initializes this result set object with the default (API server-provided) page size.
         */
        private FindDataObjectsResult(FindDataObjectsRequest requestHash, String classConstraint,
                DXEnvironment env) {
            this.baseQuery = requestHash;
            this.classConstraint = classConstraint;
            this.env = env;

            this.pageSize = null;
        }

        /**
         * Initializes this result set object with the specified page size.
         */
        private FindDataObjectsResult(FindDataObjectsRequest requestHash, String classConstraint,
                DXEnvironment env, int pageSize) {
            this.baseQuery = requestHash;
            this.classConstraint = classConstraint;
            this.env = env;

            this.pageSize = pageSize;
        }

        /**
         * Returns a {@code List} of the matching data objects.
         */
        @SuppressWarnings("unchecked")
        @Override
        public List<T> asList() {
            FindDataObjectsRequest query = new FindDataObjectsRequest(baseQuery, null, pageSize);
            List<T> output = Lists.newArrayList();

            FindDataObjectsResponse findDataObjectsResponse;

            do {
                findDataObjectsResponse =
                        DXJSON.safeTreeToValue(
                                DXAPI.systemFindDataObjects(MAPPER.valueToTree(query), env),
                                FindDataObjectsResponse.class);

                for (FindDataObjectsResponse.Entry e : findDataObjectsResponse.results) {
                    DXDataObject dataObject =
                            DXDataObject.getInstanceWithEnvironment(e.id,
                                    DXContainer.getInstance(e.project), this.env);
                    if (classConstraint != null) {
                        if (!dataObject.getId().startsWith(classConstraint + "-")) {
                            throw new IllegalStateException("Expected all results to be of type "
                                    + classConstraint + " but received an object with ID "
                                    + dataObject.getId());
                        }
                    }
                    // This is an unchecked cast, but the callers of this class
                    // should have set T appropriately so that it agrees with
                    // the class constraint (if any). If something goes wrong
                    // here, either that code is incorrect or the API server
                    // has returned incorrect results.
                    output.add((T) dataObject);
                }

                if (findDataObjectsResponse.next != null) {
                    query =
                            new FindDataObjectsRequest(query, findDataObjectsResponse.next,
                                    pageSize);
                }

            } while (findDataObjectsResponse.next != null);

            return ImmutableList.copyOf(output);

        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class FindJobsRequest {
        // Fields of the input hash to the /system/findJobs API call
        @SuppressWarnings("unused")
        @JsonProperty
        private final String launchedBy;
        @SuppressWarnings("unused")
        @JsonProperty("project")
        private final String inProject;
        private final Date createdBefore;
        private final Date createdAfter;

        @SuppressWarnings("unused")
        @JsonProperty
        private final String starting;
        @SuppressWarnings("unused")
        @JsonProperty
        private final Integer limit;

        /**
         * Creates a new {@code FindJobsRequest} that clones the specified request, but changes the
         * starting value and limit.
         *
         * @param previousQuery previous query to clone
         * @param next starting value for subsequent results
         * @param limit maximum number of results to return, or null to use the default
         *        (server-provided) limit
         */
        private FindJobsRequest(FindJobsRequest previousQuery, String next, Integer limit) {
            this.launchedBy = previousQuery.launchedBy;
            this.inProject = previousQuery.inProject;
            this.createdBefore = previousQuery.createdBefore;
            this.createdAfter = previousQuery.createdAfter;

            this.starting = next;
            this.limit = limit;
        }

        /**
         * Creates a new {@code FindJobsRequest} from the query parameters set in the specified
         * builder.
         *
         * @param builder builder object to initialize this query with
         */
        private FindJobsRequest(FindJobsRequestBuilder builder) {
            this.launchedBy = builder.launchedBy;
            this.inProject = builder.inProject;
            this.createdBefore = builder.createdBefore;
            this.createdAfter = builder.createdAfter;

            this.starting = null;
            this.limit = null;
        }

        // Getter to support JSON serialization of createdAfter.
        @SuppressWarnings("unused")
        @JsonProperty("createdAfter")
        private Long getCreatedAfter() {
            if (createdAfter == null) {
                return null;
            }
            return createdAfter.getTime();
        }

        // Getter to support JSON serialization of createdBefore.
        @SuppressWarnings("unused")
        @JsonProperty("createdBefore")
        private Long getCreatedBefore() {
            if (createdBefore == null) {
                return null;
            }
            return createdBefore.getTime();
        }

    }

    /**
     * Builder class for formulating {@code findJobs} queries and executing them.
     *
     * <p>
     * Obtain an instance of this class via {@link #findJobs()}.
     * </p>
     */
    public static class FindJobsRequestBuilder {
        private String launchedBy = null;
        private String inProject = null;
        private Date createdBefore = null;
        private Date createdAfter = null;

        private final DXEnvironment env;

        private FindJobsRequestBuilder() {
            this.env = DXEnvironment.create();
        }

        private FindJobsRequestBuilder(DXEnvironment env) {
            this.env = env;
        }

        @VisibleForTesting
        FindJobsRequest buildRequestHash() {
            // Use this method to test the JSON hash created by a particular
            // builder call without actually executing the request.
            return new FindJobsRequest(this);
        }

        /**
         * Only return jobs created after the specified date.
         *
         * @param date earliest creation date
         *
         * @return the same builder object
         */
        public FindJobsRequestBuilder createdAfter(Date date) {
            Preconditions.checkArgument(this.createdAfter == null,
                    "Cannot specify createdAfter more than once");
            Preconditions.checkNotNull(date);
            this.createdAfter = date;
            return this;
        }

        /**
         * Only return jobs created before the specified date.
         *
         * @param date latest creation date
         *
         * @return the same builder object
         */
        public FindJobsRequestBuilder createdBefore(Date date) {
            Preconditions.checkArgument(this.createdBefore == null,
                    "Cannot specify createdBefore more than once");
            Preconditions.checkNotNull(date);
            this.createdBefore = date;
            return this;
        }

        /**
         * Executes the query.
         *
         * @return object encapsulating the result set
         */
        public FindJobsResult execute() {
            return new FindJobsResult(this.buildRequestHash(), this.env);
        }

        /**
         * Executes the query with the specified page size.
         *
         * @param pageSize number of results to obtain on each request
         *
         * @return object encapsulating the result set
         */
        public FindJobsResult execute(int pageSize) {
            return new FindJobsResult(this.buildRequestHash(), this.env, pageSize);
        }

        /**
         * Only return jobs in the specified project.
         *
         * @param project project or container to search in
         *
         * @return the same builder object
         */
        public FindJobsRequestBuilder inProject(DXContainer project) {
            Preconditions.checkArgument(this.inProject == null,
                    "Cannot specify inProject more than once");
            Preconditions.checkNotNull(project);
            this.inProject = project.getId();
            return this;
        }

        /**
         * Only return jobs launched by the specified user.
         *
         * @param user user ID, e.g. {@code "user-flast"}
         *
         * @return the same builder object
         */
        public FindJobsRequestBuilder launchedBy(String user) {
            // TODO: consider changing the semantics of this and other
            // setters so that later calls overwrite earlier calls. Then
            // remove the restriction that the field may only be specified
            // once.
            Preconditions.checkArgument(this.launchedBy == null,
                    "Cannot specify launchedBy more than once");
            Preconditions.checkNotNull(user);
            this.launchedBy = user;
            return this;
        }

    }

    /**
     * Deserialized output from the /system/findJobs route.
     */
    @VisibleForTesting
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class FindJobsResponse {

        private static class Entry {
            @JsonProperty
            private String id;
        }

        @JsonProperty
        private List<Entry> results;

        @SuppressWarnings("unused")
        @JsonProperty
        private String next;

    }

    /**
     * The set of jobs that matched a {@code findJobs} query.
     *
     * <p>
     * This class paginates through the results as necessary to return the full result set.
     * </p>
     */
    public static class FindJobsResult implements ObjectProducer<DXJob> {

        private final FindJobsRequest baseQuery;
        private final DXEnvironment env;

        // Number of results to fetch with each API call, or null to use the default
        private final Integer pageSize;

        /**
         * Initializes this result set object with the default (API server-provided) page size.
         */
        private FindJobsResult(FindJobsRequest requestHash, DXEnvironment env) {
            this.baseQuery = requestHash;
            this.env = env;

            this.pageSize = null;
        }

        /**
         * Initializes this result set object with the specified page size.
         */
        private FindJobsResult(FindJobsRequest requestHash, DXEnvironment env, int pageSize) {
            this.baseQuery = requestHash;
            this.env = env;

            this.pageSize = pageSize;
        }

        /**
         * Returns a {@code List} of the matching jobs.
         */
        @Override
        public List<DXJob> asList() {
            FindJobsRequest query = new FindJobsRequest(baseQuery, null, pageSize);
            List<DXJob> output = Lists.newArrayList();
            FindJobsResponse findJobsResponse;

            do {
                findJobsResponse =
                        DXJSON.safeTreeToValue(
                                DXAPI.systemFindJobs(MAPPER.valueToTree(query), env),
                                FindJobsResponse.class);

                for (FindJobsResponse.Entry e : findJobsResponse.results) {
                    output.add(DXJob.getInstance(e.id));
                }
                if (findJobsResponse.next != null) {
                    query = new FindJobsRequest(query, findJobsResponse.next, pageSize);
                }
            } while (findJobsResponse.next != null);
            return ImmutableList.copyOf(output);
        }

    }

    /**
     * Interface that returns a sequence of {@code DXObjects}.
     *
     * @param <T> type of object to be returned
     */
    public static interface ObjectProducer<T extends DXObject> {
        /**
         * Returns a list of the matching items.
         *
         * @return List of matching items
         */
        public List<T> asList();

        // In the future we'd like to support streaming access to the results.
        // This can be done by adding a new method here, e.g.
        // public Iterable<T> asIterable();
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Returns a builder object for finding data objects that match certain criteria.
     *
     * <p>
     * Example use:
     * </p>
     *
     * <pre>
     * FindDataObjectsResponse&lt;DXDataObject&gt; fdor = DXSearch.findDataObjects().inProject(&quot;project-000000000000000000000000&quot;)
     *         .inFolder(&quot;/my/subfolder&quot;).execute();
     *
     * for (DXDataObject o : fdor.asList()) {
     *     System.out.println(o.getId());
     * }
     * </pre>
     *
     * @return a newly initialized builder object
     */
    public static FindDataObjectsRequestBuilder<DXDataObject> findDataObjects() {
        return new FindDataObjectsRequestBuilder<DXDataObject>();
    }

    /**
     * Returns a builder object for finding data objects that match certain criteria, using the
     * specified environment.
     *
     * <p>
     * Example use:
     * </p>
     *
     * <pre>
     * FindDataObjectsResponse&lt;DXDataObject&gt; fdor = DXSearch.findDataObjects(env).inProject(&quot;project-000000000000000000000000&quot;)
     *         .inFolder(&quot;/my/subfolder&quot;).execute();
     *
     * for (DXDataObject o : fdor.asList()) {
     *     System.out.println(o.getId());
     * }
     * </pre>
     *
     * @param env environment specifying API server parameters for issuing the query; the
     *        environment will be propagated into objects that are subsequently returned
     *
     * @return a newly initialized builder object
     */
    public static FindDataObjectsRequestBuilder<DXDataObject> findDataObjectsWithEnvironment(
            DXEnvironment env) {
        return new FindDataObjectsRequestBuilder<DXDataObject>(env);
    }

    /**
     * Returns a builder object for finding jobs that match certain criteria.
     *
     * <p>
     * Example use:
     * </p>
     *
     * <pre>
     * FindJobsResponse fjr = DXSearch.findJobs().launchedBy(&quot;user-dnanexus&quot;).inProject(&quot;project-000000000000000000000000&quot;)
     *         .createdBefore(new GregorianCalendar(2012, 11, 31).getTime()).execute();
     *
     * for (DXJob job : fjr.asList()) {
     *     System.out.println(job.getId());
     * }
     * </pre>
     *
     * @return a newly initialized builder object
     */
    public static FindJobsRequestBuilder findJobs() {
        return new FindJobsRequestBuilder();
    }

    /**
     * Returns a builder object for finding jobs that match certain criteria, using the specified
     * environment.
     *
     * <p>
     * Example use:
     * </p>
     *
     * <pre>
     * FindJobsResponse fjr = DXSearch.findJobs().launchedBy(&quot;user-dnanexus&quot;).inProject(&quot;project-000000000000000000000000&quot;)
     *         .createdBefore(new GregorianCalendar(2012, 11, 31).getTime()).execute();
     *
     * for (DXJob job : fjr.asList()) {
     *     System.out.println(job.getId());
     * }
     * </pre>
     *
     * @param env environment specifying API server parameters for issuing the query; the
     *        environment will be propagated into objects that are subsequently returned
     *
     * @return a newly initialized builder object
     */
    public static FindJobsRequestBuilder findJobsWithEnvironment(DXEnvironment env) {
        return new FindJobsRequestBuilder(env);
    }

    // Prevent this utility class from being instantiated.
    private DXSearch() {}

}
