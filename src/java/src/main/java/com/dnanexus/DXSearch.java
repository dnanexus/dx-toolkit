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

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Utility class containing methods for searching for platform objects by various criteria.
 */
public final class DXSearch {

    /**
     * Specifies in a query whether to return visible items, hidden items, or both.
     */
    public enum VisibilityQuery {
        /**
         * Search for only visible items.
         */
        VISIBLE("visible"),
        /**
         * Search for only hidden items.
         */
        HIDDEN("hidden"),
        /**
         * Search for both hidden and visible items.
         */
        EITHER("either");

        private String value;

        private VisibilityQuery(String value) {
            this.value = value;
        }

        @SuppressWarnings("unused")
        @JsonValue
        private String getValue() {
            return this.value;
        }
    }

    /**
     * A request to the /system/findDataObjects route.
     */
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

        // This interface, and the classes that implement it, are for
        // generating the values that can appear in the "name" field of the
        // query.
        @JsonInclude(Include.NON_NULL)
        private static interface NameQuery {
            // Subclasses choose what fields to put in their JSON representations.
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
                this(projectId, folder, false);
            }

            private ScopeQuery(String projectId, String folder, Boolean recurse) {
                this.project = projectId;
                this.folder = folder;
                this.recurse = recurse;
            }
        }

        @JsonInclude(Include.NON_NULL)
        private static class TimeIntervalQuery {
            private final Date before;
            private final Date after;

            private TimeIntervalQuery(Date before, Date after) {
                this.before = before;
                this.after = after;
            }

            @SuppressWarnings("unused")
            @JsonProperty("after")
            private Long getAfter() {
                if (after == null) {
                    return null;
                }
                return after.getTime();
            }

            @SuppressWarnings("unused")
            @JsonProperty("before")
            private Long getBefore() {
                if (before == null) {
                    return null;
                }
                return before.getTime();
            }
        }

        @JsonProperty("class")
        private final String classConstraint;
        @JsonProperty
        private final DataObjectState state;
        @JsonProperty
        private final VisibilityQuery visibility;
        @JsonProperty
        private final NameQuery name;
        // TODO: support $and and $or queries on type, not just a single type
        @JsonProperty
        private final String type;
        // TODO: support $and and $or queries on tags, not just a single tag
        @JsonProperty
        private final String tags;
        @JsonProperty
        private final Map<String, Object> properties;
        @JsonProperty
        private final String link;
        @JsonProperty
        private final ScopeQuery scope;
        @JsonProperty
        private final AccessLevel level;
        @JsonProperty
        private final TimeIntervalQuery modified;
        @JsonProperty
        private final TimeIntervalQuery created;

        // TODO: describe

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
            this.classConstraint = previousQuery.classConstraint;
            this.state = previousQuery.state;
            this.visibility = previousQuery.visibility;
            this.name = previousQuery.name;
            this.type = previousQuery.type;
            this.tags = previousQuery.tags;
            this.properties = previousQuery.properties;
            this.link = previousQuery.link;
            this.scope = previousQuery.scope;
            this.level = previousQuery.level;
            this.modified = previousQuery.modified;
            this.created = previousQuery.created;

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
            this.classConstraint = builder.classConstraint;
            this.state = builder.state;
            this.visibility = builder.visibilityQuery;
            this.name = builder.nameQuery;
            this.type = builder.type;
            this.tags = builder.tag;

            Map<String, Object> properties =
                    Maps.<String, Object>newHashMap(builder.propertyKeysAndValues);
            for (String requiredKey : builder.propertiesThatMustBePresent) {
                properties.put(requiredKey, true);
            }
            if (!properties.isEmpty()) {
                this.properties = Collections.unmodifiableMap(properties);
            } else {
                this.properties = null;
            }

            this.link = builder.link;
            this.scope = builder.scopeQuery;
            this.level = builder.level;
            if (builder.modifiedBefore != null || builder.modifiedAfter != null) {
                this.modified =
                        new TimeIntervalQuery(builder.modifiedBefore, builder.modifiedAfter);
            } else {
                this.modified = null;
            }
            if (builder.createdBefore != null || builder.createdAfter != null) {
                this.created = new TimeIntervalQuery(builder.createdBefore, builder.createdAfter);
            } else {
                this.created = null;
            }

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
        private DataObjectState state;
        private VisibilityQuery visibilityQuery;
        private FindDataObjectsRequest.NameQuery nameQuery;
        private String type;
        private String tag;
        private Map<String, String> propertyKeysAndValues = Maps.newHashMap();
        private Set<String> propertiesThatMustBePresent = Sets.newHashSet();
        private String link;
        private FindDataObjectsRequest.ScopeQuery scopeQuery;
        private AccessLevel level;
        private Date modifiedBefore;
        private Date modifiedAfter;
        private Date createdBefore;
        private Date createdAfter;

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
         * Only returns data objects that were created after the specified time.
         *
         * @param createdAfter creation date
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> createdAfter(Date createdAfter) {
            Preconditions.checkState(this.createdAfter == null,
                    "Cannot call createdAfter more than once");
            this.createdAfter =
                    Preconditions.checkNotNull(createdAfter, "createdAfter may not be null");
            return this;
        }

        /**
         * Only returns data objects that were created before the specified time.
         *
         * @param createdBefore creation date
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> createdBefore(Date createdBefore) {
            Preconditions.checkState(this.createdBefore == null,
                    "Cannot call createdBefore more than once");
            this.createdBefore =
                    Preconditions.checkNotNull(createdBefore, "createdBefore may not be null");
            return this;
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
            Preconditions.checkState(this.scopeQuery == null,
                    "Cannot specify inProject, inFolder, or inFolderOrSubfolders more than once");;;
            this.scopeQuery =
                    new FindDataObjectsRequest.ScopeQuery(Preconditions.checkNotNull(project,
                            "project may not be null").getId(), Preconditions.checkNotNull(folder,
                            "folder may not be null"));
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
            Preconditions.checkState(this.scopeQuery == null,
                    "Cannot specify inProject, inFolder, or inFolderOrSubfolders more than once");
            this.scopeQuery =
                    new FindDataObjectsRequest.ScopeQuery(Preconditions.checkNotNull(project,
                            "project may not be null").getId(), Preconditions.checkNotNull(folder,
                            "folder may not be null"), true);
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
            Preconditions.checkState(this.scopeQuery == null,
                    "Cannot specify inProject, inFolder, or inFolderOrSubfolders more than once");
            this.scopeQuery =
                    new FindDataObjectsRequest.ScopeQuery(Preconditions.checkNotNull(project,
                            "project may not be null").getId());
            return this;
        }

        /**
         * Only returns data objects that were last modified after the specified time.
         *
         * @param modifiedAfter last modified date
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> modifiedAfter(Date modifiedAfter) {
            Preconditions.checkState(this.modifiedAfter == null,
                    "Cannot call modifiedAfter more than once");
            this.modifiedAfter =
                    Preconditions.checkNotNull(modifiedAfter, "modifiedAfter may not be null");
            return this;
        }

        /**
         * Only returns data objects that were last modified before the specified time.
         *
         * @param modifiedBefore last modified date
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> modifiedBefore(Date modifiedBefore) {
            Preconditions.checkState(this.modifiedBefore == null,
                    "Cannot call modifiedBefore more than once");
            this.modifiedBefore =
                    Preconditions.checkNotNull(modifiedBefore, "modifiedBefore may not be null");
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
            Preconditions.checkState(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            this.nameQuery =
                    new FindDataObjectsRequest.ExactNameQuery(Preconditions.checkNotNull(name,
                            "name may not be null"));
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
            Preconditions.checkState(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            this.nameQuery =
                    new FindDataObjectsRequest.GlobNameQuery(Preconditions.checkNotNull(glob,
                            "glob may not be null"));
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
            Preconditions.checkState(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            this.nameQuery =
                    new FindDataObjectsRequest.RegexpNameQuery(Preconditions.checkNotNull(regexp,
                            "regexp may not be null"));
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
            Preconditions.checkState(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            this.nameQuery =
                    new FindDataObjectsRequest.RegexpNameQuery(Preconditions.checkNotNull(regexp,
                            "regexp may not be null"), caseInsensitive ? "i" : null);
            return this;
        }

        /**
         * Only returns applets (filters out data objects of all other classes).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #withClassFile()}, {@link #withClassGTable()},
         * {@link #withClassRecord()}, and {@link #withClassWorkflow()}.
         * </p>
         *
         * @return the same builder object
         */
        @SuppressWarnings("unchecked")
        public FindDataObjectsRequestBuilder<DXApplet> withClassApplet() {
            Preconditions.checkState(this.classConstraint == null,
                    "Cannot specify class constraints more than once");
            this.classConstraint = "applet";
            // This cast should be safe, since we hold no references of type T
            return (FindDataObjectsRequestBuilder<DXApplet>) this;
        }

        /**
         * Only returns files (filters out data objects of all other classes).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #withClassApplet()}, {@link #withClassGTable()},
         * {@link #withClassRecord()}, and {@link #withClassWorkflow()}.
         * </p>
         *
         * @return the same builder object
         */
        @SuppressWarnings("unchecked")
        public FindDataObjectsRequestBuilder<DXFile> withClassFile() {
            Preconditions.checkState(this.classConstraint == null,
                    "Cannot specify class constraints more than once");
            this.classConstraint = "file";
            // This cast should be safe, since we hold no references of type T
            return (FindDataObjectsRequestBuilder<DXFile>) this;
        }

        /**
         * Only returns GTables (filters out data objects of all other classes).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #withClassApplet()}, {@link #withClassFile()},
         * {@link #withClassRecord()}, and {@link #withClassWorkflow()}.
         * </p>
         *
         * @return the same builder object
         */
        @SuppressWarnings("unchecked")
        public FindDataObjectsRequestBuilder<DXGTable> withClassGTable() {
            Preconditions.checkState(this.classConstraint == null,
                    "Cannot specify class constraints more than once");
            this.classConstraint = "gtable";
            // This cast should be safe, since we hold no references of type T
            return (FindDataObjectsRequestBuilder<DXGTable>) this;
        }

        /**
         * Only returns records (filters out data objects of all other classes).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #withClassApplet()}, {@link #withClassFile()},
         * {@link #withClassGTable()}, and {@link #withClassWorkflow()}.
         * </p>
         *
         * @return the same builder object
         */
        @SuppressWarnings("unchecked")
        public FindDataObjectsRequestBuilder<DXRecord> withClassRecord() {
            Preconditions.checkState(this.classConstraint == null,
                    "Cannot specify class constraints more than once");
            this.classConstraint = "record";
            // This cast should be safe, since we hold no references of type T
            return (FindDataObjectsRequestBuilder<DXRecord>) this;
        }

        /**
         * Only returns workflows (filters out data objects of all other classes).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #withClassApplet()}, {@link #withClassFile()},
         * {@link #withClassGTable()}, and {@link #withClassRecord()}.
         * </p>
         *
         * @return the same builder object
         */
        @SuppressWarnings("unchecked")
        public FindDataObjectsRequestBuilder<DXWorkflow> withClassWorkflow() {
            Preconditions.checkState(this.classConstraint == null,
                    "Cannot specify class constraints more than once");
            this.classConstraint = "workflow";
            // This cast should be safe, since we hold no references of type T
            return (FindDataObjectsRequestBuilder<DXWorkflow>) this;
        }

        /**
         * Only returns data objects that link to the specified data object.
         *
         * @param dataObject data object that must be the target of a DNAnexus link on matching data
         *        objects
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withLinkTo(DXDataObject dataObject) {
            Preconditions.checkState(this.link == null, "Cannot call withLinkTo more than once");
            this.link =
                    Preconditions.checkNotNull(dataObject, "dataObject may not be null").getId();
            return this;
        }

        /**
         * Only returns data objects to which the requesting user has at least the specified level
         * of permission in some project.
         *
         * @param level project access level (must be greater than NONE)
         *
         * @return the same data object
         */
        public FindDataObjectsRequestBuilder<T> withMinimumAccessLevel(AccessLevel level) {
            Preconditions.checkState(this.level == null,
                    "Cannot call withMinimumAccessLevel more than once");
            Preconditions.checkNotNull(level, "level may not be null");
            Preconditions.checkArgument(!level.equals(AccessLevel.NONE),
                    "Minimum access level may not be NONE");
            this.level = level;
            return this;
        }

        /**
         * Only returns data objects where the specified property is present.
         *
         * @param propertyKey property key that must be present
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withProperty(String propertyKey) {
            propertiesThatMustBePresent.add(Preconditions.checkNotNull(propertyKey,
                    "propertyKey may not be null"));
            return this;
        }

        /**
         * Only returns data objects where the specified property has the specified value.
         *
         * @param propertyKey property key
         * @param propertyValue property value
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withProperty(String propertyKey,
                String propertyValue) {
            propertyKeysAndValues.put(
                    Preconditions.checkNotNull(propertyKey, "propertyKey may not be null"),
                    Preconditions.checkNotNull(propertyValue, "propertyValue may not be null"));
            return this;
        }

        /**
         * Only returns data objects with the specified state.
         *
         * @param state data object state
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withState(DataObjectState state) {
            Preconditions.checkState(this.state == null, "Cannot call withState more than once");
            this.state = Preconditions.checkNotNull(state, "state may not be null");
            return this;
        }

        /**
         * Only returns data objects with the specified tag.
         *
         * @param tag String containing a tag
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withTag(String tag) {
            Preconditions.checkState(this.tag == null, "Cannot call withTag more than once");
            this.tag = Preconditions.checkNotNull(tag, "tag may not be null");
            return this;
        }

        /**
         * Only returns data objects with the specified type.
         *
         * @param type String containing a type
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withType(String type) {
            Preconditions.checkState(this.type == null, "Cannot call withType more than once");
            this.type = Preconditions.checkNotNull(type, "type may not be null");
            return this;
        }

        /**
         * Only returns data objects with the specified visibility (visible or hidden). If not
         * specified, the default is to return only visible objects.
         *
         * @param visibilityQuery enum value specifying what visibility/ies can be returned.
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withVisibility(VisibilityQuery visibilityQuery) {
            Preconditions.checkState(this.visibilityQuery == null,
                    "Cannot call withVisibility more than once");
            this.visibilityQuery =
                    Preconditions.checkNotNull(visibilityQuery, "visibilityQuery may not be null");
            return this;
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

        @JsonProperty
        private Entry next;

    }

    /**
     * The set of data objects that matched a {@code findDataObjects} query.
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
                        DXAPI.systemFindDataObjects(query, FindDataObjectsResponse.class, env);

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

    /**
     * A request to the /system/findJobs route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class FindJobsRequest {
        @JsonProperty
        private final String launchedBy;
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
            Preconditions.checkState(this.createdAfter == null,
                    "Cannot specify createdAfter more than once");
            this.createdAfter = Preconditions.checkNotNull(date, "date may not be null");
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
            Preconditions.checkState(this.createdBefore == null,
                    "Cannot specify createdBefore more than once");
            this.createdBefore = Preconditions.checkNotNull(date, "date may not be null");
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
            Preconditions.checkState(this.inProject == null,
                    "Cannot specify inProject more than once");
            this.inProject = Preconditions.checkNotNull(project, "project may not be null").getId();
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
            Preconditions.checkState(this.launchedBy == null,
                    "Cannot specify launchedBy more than once");
            this.launchedBy = Preconditions.checkNotNull(user, "user may not be null");
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
                        DXAPI.systemFindJobs(MAPPER.valueToTree(query), FindJobsResponse.class, env);

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
