// Copyright (C) 2013-2014 DNAnexus, Inc.
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

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Utility class containing methods for searching for platform objects by various criteria.
 */
public final class DXSearch {

    /**
     * Specifies whether describe output should be returned with the find* request (and if so, with
     * what describe options).
     */
    @JsonInclude(Include.NON_NULL)
    private static class DescribeParameters {
        private final Object describeOptions;

        private DescribeParameters() {
            this.describeOptions = null;
        }

        private DescribeParameters(DXDataObject.DescribeOptions describeOptions) {
            this.describeOptions = describeOptions;
        }

        @SuppressWarnings("unused")
        @JsonValue
        private Object getValue() {
            if (describeOptions == null) {
                return true;
            }
            return describeOptions;
        }
    }

    /**
     * A request to the /system/findDataObjects route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class FindDataObjectsRequest {

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

        @JsonProperty
        private final List<String> id;
        @JsonProperty("class")
        private final String classConstraint;
        @JsonProperty
        private final DataObjectState state;
        @JsonProperty
        private final VisibilityQuery visibility;
        @JsonProperty
        private final NameQuery name;
        @JsonProperty
        private final TypeQuery type;
        @JsonProperty
        private final TagsQuery tags;
        @JsonProperty
        private final PropertiesQuery properties;
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

        @JsonProperty
        private final DescribeParameters describe;

        @SuppressWarnings("unused")
        @JsonProperty
        private final JsonNode starting;
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
                JsonNode next, Integer limit) {
            this.classConstraint = previousQuery.classConstraint;
            this.id = previousQuery.id;
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
            this.describe = previousQuery.describe;

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
            this.id = builder.id;
            this.state = builder.state;
            this.visibility = builder.visibilityQuery;
            this.name = builder.nameQuery;
            this.type = builder.type;
            this.tags = builder.tags;
            this.describe = builder.describe;
            // For backwards compatibility we allow withProperty to be specified more than once,
            // with the different conditions being implicitly $and'ed.
            if (builder.properties.size() == 0) {
                this.properties = null;
            } else if (builder.properties.size() == 1) {
                this.properties = builder.properties.get(0);
            } else {
                this.properties = PropertiesQuery.allOf(builder.properties);
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
        private List<String> id;
        private DataObjectState state;
        private VisibilityQuery visibilityQuery;
        private NameQuery nameQuery;
        private TypeQuery type;
        private TagsQuery tags;
        private List<PropertiesQuery> properties = Lists.newArrayList(); // Implicit $and
        private String link;
        private FindDataObjectsRequest.ScopeQuery scopeQuery;
        private AccessLevel level;
        private Date modifiedBefore;
        private Date modifiedAfter;
        private Date createdBefore;
        private Date createdAfter;
        private DescribeParameters describe;

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
         * Requests the default describe data for each matching data object when the query is run.
         * The {@link DXDataObject#getCachedDescribe()} method can be used if, and only if, this
         * method is called at query time.
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> includeDescribeOutput() {
            Preconditions.checkState(this.describe == null,
                    "Cannot specify describe output more than once");
            this.describe = new DescribeParameters();
            return this;
        }

        /**
         * Requests describe data (with the specified options) for each matching data object when
         * the query is run. The {@link DXDataObject#getCachedDescribe()} method can be used if, and
         * only if, this method is called at query time.
         *
         * @param describeOptions options specifying which fields to be returned or a project hint
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> includeDescribeOutput(
                DXDataObject.DescribeOptions describeOptions) {
            Preconditions.checkState(this.describe == null,
                    "Cannot specify describe output more than once");
            this.describe = new DescribeParameters(describeOptions);
            return this;
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
                    "Cannot specify inProject, inFolder, or inFolderOrSubfolders more than once");
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
                    new NameQuery.ExactNameQuery(Preconditions.checkNotNull(name,
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
                    new NameQuery.GlobNameQuery(Preconditions.checkNotNull(glob,
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
                    new NameQuery.RegexpNameQuery(Preconditions.checkNotNull(regexp,
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
                    new NameQuery.RegexpNameQuery(Preconditions.checkNotNull(regexp,
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
         * Only returns data objects whose IDs match one of the specified data objects.
         *
         * <p>
         * When used in combination with {@link #includeDescribeOutput()} or
         * {@link #includeDescribeOutput(com.dnanexus.DXDataObject.DescribeOptions)} you can use
         * this to "bulk describe" a large number of objects. Note that hidden objects among the
         * requested list will not be returned unless you use
         * {@link #withVisibility(DXSearch.VisibilityQuery)} to request all objects.
         * </p>
         *
         * <p>
         * The number of results may exceed the number of unique data objects supplied because each
         * result appears once for each accessible project it can be found in (you can call
         * {@link #inProject(DXContainer)} to avoid this multiplicity).
         * </p>
         *
         * @param dataObjects collection of data objects
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withIdsIn(
                Collection<? extends DXDataObject> dataObjects) {
            Preconditions.checkState(this.id == null, "Cannot call withIdsIn more than once");
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            // We can't use the default serialization here because we don't want everything to get
            // wrapped up in $dnanexus_link
            for (DXDataObject d : dataObjects) {
                builder.add(d.getId());
            }
            this.id = builder.build();
            return this;
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
         * Only returns data objects matching the specified properties query.
         *
         * @param propertiesQuery properties query
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withProperties(PropertiesQuery propertiesQuery) {
            // Multiple calls to withProperty are allowed here for backwards compatiblity (this was
            // how you could query on multiple properties before PropertiesQuery.allOf was
            // introduced). However, other fields don't support analogous functionality and it could
            // be removed some time in the future.
            this.properties.add(Preconditions.checkNotNull(propertiesQuery));
            return this;
        }

        /**
         * Only returns data objects where the specified property is present.
         *
         * <p>
         * To specify a complex query on the properties, use
         * {@link #withProperties(DXSearch.PropertiesQuery)}.
         * </p>
         *
         * @param propertyKey property key that must be present
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withProperty(String propertyKey) {
            return withProperties(PropertiesQuery.withKey(propertyKey));
        }

        /**
         * Only returns data objects where the specified property has the specified value.
         *
         * <p>
         * To specify a complex query on the properties, use
         * {@link #withProperties(DXSearch.PropertiesQuery)}.
         * </p>
         *
         * @param propertyKey property key
         * @param propertyValue property value
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withProperty(String propertyKey,
                String propertyValue) {
            return withProperties(PropertiesQuery.withKeyAndValue(propertyKey, propertyValue));
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
         * <p>
         * To specify a complex query on the tags, use {@link #withTags(DXSearch.TagsQuery)}.
         * </p>
         *
         * @param tag String containing a tag
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withTag(String tag) {
            Preconditions.checkState(this.tags == null, "Cannot specify withTag* more than once");
            this.tags = TagsQuery.of(Preconditions.checkNotNull(tag, "tag may not be null"));
            return this;
        }

        /**
         * Only returns data objects matching the specified tags query.
         *
         * @param tagsQuery tags query
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withTags(TagsQuery tagsQuery) {
            Preconditions.checkState(this.tags == null, "Cannot specify withTag* more than once");
            this.tags = Preconditions.checkNotNull(tagsQuery, "tagsQuery may not be null");
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
            this.type = TypeQuery.of(Preconditions.checkNotNull(type, "type may not be null"));
            return this;
        }

        /**
         * Only returns data objects matching the specified types query.
         *
         * @param typeQuery types query
         *
         * @return the same builder object
         */
        public FindDataObjectsRequestBuilder<T> withTypes(TypeQuery typeQuery) {
            Preconditions.checkState(this.type == null, "Cannot specify withType* more than once");
            this.type = Preconditions.checkNotNull(typeQuery, "typeQuery may not be null");
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
     * Deserialized output from the /system/findDataObjects route.
     */
    @VisibleForTesting
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class FindDataObjectsResponse {

        @JsonIgnoreProperties(ignoreUnknown = true)
        private static class Entry {
            @JsonProperty
            private String id;
            @JsonProperty
            private String project;
            @JsonProperty
            private JsonNode describe;
        }

        @JsonProperty
        private List<Entry> results;

        @JsonProperty
        private JsonNode next;

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
    public static class FindDataObjectsResult<T extends DXDataObject> extends ObjectProducerImpl<T> {

        /**
         * Wrapper from the findDataObjects result page class to the high-level interface
         * FindResultPage.
         */
        private class FindDataObjectsResultPage implements FindResultPage<T> {

            private final FindDataObjectsResponse response;

            public FindDataObjectsResultPage(FindDataObjectsResponse response) {
                this.response = response;
            }

            @Override
            public T get(int index) {
                return getDataObjectInstanceFromResult(response.results.get(index));
            }

            @Override
            public boolean hasNextPage() {
                return response.next != null;
            }

            @Override
            public int size() {
                return response.results.size();
            }

        }

        /**
         * Iterator implementation for findDataObjects results.
         */
        private class ResultIterator
                extends
                PaginatingFindResultIterator<T, FindDataObjectsRequest, FindDataObjectsResultPage> {

            public ResultIterator() {
                super(baseQuery);
            }

            @Override
            public FindDataObjectsRequest getNextQuery(FindDataObjectsRequest query,
                    FindDataObjectsResultPage currentResultPage) {
                return new FindDataObjectsRequest(query, currentResultPage.response.next, pageSize);
            }

            @Override
            public FindDataObjectsResultPage issueQuery(FindDataObjectsRequest query) {
                return new FindDataObjectsResultPage(DXAPI.systemFindDataObjects(query,
                        FindDataObjectsResponse.class, env));
            }
        }

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

        @SuppressWarnings("unchecked")
        private T getDataObjectInstanceFromResult(FindDataObjectsResponse.Entry e) {
            DXDataObject dataObject = null;
            DXContainer container = DXContainer.getInstance(e.project);
            if (e.describe != null) {
                dataObject =
                        DXDataObject.getInstanceWithCachedDescribe(e.id, container, this.env,
                                e.describe);
            } else {
                dataObject = DXDataObject.getInstanceWithEnvironment(e.id, container, this.env);
            }

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
            return (T) dataObject;
        }


        @Override
        public Iterator<T> iterator() {
            return new ResultIterator();
        }
    }

    /**
     * A request to the /system/findExecutions route.
     */
    @JsonInclude(Include.NON_NULL)
    private static class FindExecutionsRequest {
        @JsonProperty("class")
        private final String classConstraint;
        @JsonProperty
        private final List<String> id;
        @JsonProperty
        private final String launchedBy;
        @JsonProperty
        private final String project;
        @JsonProperty
        private final TimeIntervalQuery created;
        @JsonProperty
        private final Boolean includeSubjobs;
        @JsonProperty
        private final NameQuery name;
        @JsonProperty
        private final String executable;
        @JsonProperty
        private final TagsQuery tags;
        @JsonProperty
        private final PropertiesQuery properties;
        @JsonProperty
        private final String rootExecution;
        @JsonProperty
        private final String originJob;
        @JsonProperty
        private final String parentJob;
        @JsonProperty
        private final String parentAnalysis;
        /**
         * Desired execution state(s). In general could be a list of Strings, but if it's a
         * singleton array we just pass the String alone.
         */
        @JsonProperty
        private final Object state;

        @JsonProperty
        private final DescribeParameters describe;

        @SuppressWarnings("unused")
        @JsonProperty
        private final JsonNode starting;
        @SuppressWarnings("unused")
        @JsonProperty
        private final Integer limit;

        /**
         * Creates a new request that clones the specified request, but changes the starting value
         * and limit.
         *
         * @param previousQuery previous query to clone
         * @param next starting value for subsequent results
         * @param limit maximum number of results to return, or null to use the default
         *        (server-provided) limit
         */
        private FindExecutionsRequest(FindExecutionsRequest previousQuery, JsonNode next,
                Integer limit) {
            this.classConstraint = previousQuery.classConstraint;
            this.id = previousQuery.id;
            this.launchedBy = previousQuery.launchedBy;
            this.project = previousQuery.project;
            this.created = previousQuery.created;
            this.includeSubjobs = previousQuery.includeSubjobs;
            this.name = previousQuery.name;
            this.executable = previousQuery.executable;
            this.tags = previousQuery.tags;
            this.properties = previousQuery.properties;
            this.rootExecution = previousQuery.rootExecution;
            this.originJob = previousQuery.originJob;
            this.parentJob = previousQuery.parentJob;
            this.parentAnalysis = previousQuery.parentAnalysis;
            this.state = previousQuery.state;

            this.describe = previousQuery.describe;

            this.starting = next;
            this.limit = limit;
        }

        /**
         * Creates a new request from the query parameters set in the specified builder.
         *
         * @param builder builder object to initialize this query with
         */
        private FindExecutionsRequest(FindExecutionsRequestBuilder<?> builder) {
            this.classConstraint = builder.classConstraint;
            this.id = builder.id;
            this.launchedBy = builder.launchedBy;
            this.project = builder.inProject;
            this.includeSubjobs = builder.includeSubjobs;
            this.name = builder.nameQuery;
            this.executable = builder.executable;
            this.tags = builder.tags;
            // For backwards compatibility we allow withProperty to be specified more than once,
            // with the different conditions being implicitly $and'ed.
            if (builder.properties.size() == 0) {
                this.properties = null;
            } else if (builder.properties.size() == 1) {
                this.properties = builder.properties.get(0);
            } else {
                this.properties = PropertiesQuery.allOf(builder.properties);
            }
            this.rootExecution = builder.rootExecution;
            this.originJob = builder.originJob;
            this.parentJob = builder.parentJob;
            this.parentAnalysis = builder.parentAnalysis;

            if (builder.createdBefore != null || builder.createdAfter != null) {
                this.created = new TimeIntervalQuery(builder.createdBefore, builder.createdAfter);
            } else {
                this.created = null;
            }

            int totalNumStates = builder.jobStates.size() + builder.analysisStates.size();
            if (totalNumStates > 0) {
                if (totalNumStates > 1) {
                    // TODO: if jobStates and analysisStates are both non-empty we might want to
                    // deduplicate state names that happen to be the same.
                    this.state =
                            ImmutableList.builder().addAll(builder.jobStates)
                                    .addAll(builder.analysisStates).build();
                } else {
                    // Exactly one of jobStates or analysisStates has a single element, and the
                    // other one is empty. In the case of only one element, we fill in just the
                    // String instead of a singleton array.
                    this.state =
                            Iterables.getOnlyElement(builder.jobStates.size() == 1
                                    ? builder.jobStates
                                    : builder.analysisStates);
                }
            } else {
                this.state = null;
            }

            this.describe = builder.describe;

            this.starting = null;
            this.limit = null;
        }
    }

    /**
     * Builder class for formulating {@code findExecutions} queries and executing them.
     *
     * <p>
     * Obtain an instance of this class via {@link #findExecutions()}.
     * </p>
     *
     * @param <T> execution class that will be returned from the query
     */
    public static class FindExecutionsRequestBuilder<T extends DXExecution> {
        private String classConstraint;
        private List<String> id;
        private String launchedBy;
        private String inProject;
        private Date createdBefore;
        private Date createdAfter;
        private Boolean includeSubjobs;
        private NameQuery nameQuery;
        private String executable;
        private TagsQuery tags;
        private List<PropertiesQuery> properties = Lists.newArrayList(); // Implicit $and
        private String rootExecution;
        private String originJob;
        private String parentJob;
        private String parentAnalysis;
        private List<JobState> jobStates = Lists.newArrayList();
        private List<AnalysisState> analysisStates = Lists.newArrayList();

        private DescribeParameters describe;

        private final DXEnvironment env;

        private FindExecutionsRequestBuilder() {
            this.env = DXEnvironment.create();
        }

        private FindExecutionsRequestBuilder(DXEnvironment env) {
            this.env = env;
        }

        @VisibleForTesting
        FindExecutionsRequest buildRequestHash() {
            // Use this method to test the JSON hash created by a particular
            // builder call without actually executing the request.
            return new FindExecutionsRequest(this);
        }

        /**
         * Only return executions created after the specified date.
         *
         * @param date earliest creation date
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> createdAfter(Date date) {
            Preconditions.checkState(this.createdAfter == null,
                    "Cannot specify createdAfter more than once");
            this.createdAfter = Preconditions.checkNotNull(date, "date may not be null");
            return this;
        }

        /**
         * Only return executions created before the specified date.
         *
         * @param date latest creation date
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> createdBefore(Date date) {
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
        public FindExecutionsResult<T> execute() {
            return new FindExecutionsResult<T>(this.buildRequestHash(), this.classConstraint,
                    this.env);
        }

        /**
         * Executes the query with the specified page size.
         *
         * @param pageSize number of results to obtain on each request
         *
         * @return object encapsulating the result set
         */
        public FindExecutionsResult<T> execute(int pageSize) {
            return new FindExecutionsResult<T>(this.buildRequestHash(), this.classConstraint,
                    this.env, pageSize);
        }

        /**
         * Requests the default describe data for each matching execution when the query is run. The
         * {@link DXExecution#getCachedDescribe()} method can be used if, and only if, this method
         * is called at query time.
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> includeDescribeOutput() {
            // TODO: when DXJobs and DXAnalyses support DescribeOptions, this method should receive
            // another overload that allows specifying options
            Preconditions.checkState(this.describe == null,
                    "Cannot specify describe output more than once");
            this.describe = new DescribeParameters();
            return this;
        }

        /**
         * Specifies whether subjobs should be included among the results (default is true). If
         * false, only non-subjob executions (i.e., master jobs, origin jobs, and analyses) will be
         * returned.
         *
         * @param includeSubjobs whether to include subjobs in the results
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> includeSubjobs(boolean includeSubjobs) {
            Preconditions.checkState(this.includeSubjobs == null,
                    "Cannot specify includeSubjobs more than once");
            this.includeSubjobs = includeSubjobs;
            return this;
        }

        /**
         * Only returns executions in the specified project.
         *
         * @param project project or container to search in
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> inProject(DXContainer project) {
            Preconditions.checkState(this.inProject == null,
                    "Cannot specify inProject more than once");
            this.inProject = Preconditions.checkNotNull(project, "project may not be null").getId();
            return this;
        }

        /**
         * Only returns executions launched by the specified user.
         *
         * @param user user ID, e.g. {@code "user-flast"}
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> launchedBy(String user) {
            Preconditions.checkState(this.launchedBy == null,
                    "Cannot specify launchedBy more than once");
            this.launchedBy = Preconditions.checkNotNull(user, "user may not be null");
            return this;
        }

        /**
         * Only returns executions whose names exactly equal the specified string.
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #nameMatchesGlob(String)}, {@link #nameMatchesRegexp(String)}, and
         * {@link #nameMatchesRegexp(String, boolean)}.
         * </p>
         *
         * @param name name of execution
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> nameMatchesExactly(String name) {
            Preconditions.checkState(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            this.nameQuery =
                    new NameQuery.ExactNameQuery(Preconditions.checkNotNull(name,
                            "name may not be null"));
            return this;
        }

        /**
         * Only returns executions whose names match the specified glob.
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #nameMatchesExactly(String)}, {@link #nameMatchesRegexp(String)},
         * and {@link #nameMatchesRegexp(String, boolean)}.
         * </p>
         *
         * @param glob shell-like pattern to be matched against execution name
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> nameMatchesGlob(String glob) {
            Preconditions.checkState(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            this.nameQuery =
                    new NameQuery.GlobNameQuery(Preconditions.checkNotNull(glob,
                            "glob may not be null"));
            return this;
        }

        /**
         * Only returns executions whose names match the specified regexp.
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #nameMatchesExactly(String)}, {@link #nameMatchesGlob(String)}, and
         * {@link #nameMatchesRegexp(String, boolean)}.
         * </p>
         *
         * @param regexp regexp to be matched against execution name
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> nameMatchesRegexp(String regexp) {
            Preconditions.checkState(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            this.nameQuery =
                    new NameQuery.RegexpNameQuery(Preconditions.checkNotNull(regexp,
                            "regexp may not be null"));
            return this;
        }

        /**
         * Only returns executions whose names match the specified regexp (optionally allowing the
         * match to be case insensitive).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #nameMatchesExactly(String)}, {@link #nameMatchesGlob(String)}, and
         * {@link #nameMatchesRegexp(String)}.
         * </p>
         *
         * @param regexp regexp to be matched against execution name
         * @param caseInsensitive if true, the regexp is matched case-insensitively
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> nameMatchesRegexp(String regexp,
                boolean caseInsensitive) {
            Preconditions.checkState(this.nameQuery == null,
                    "Cannot specify nameMatches* methods more than once");
            this.nameQuery =
                    new NameQuery.RegexpNameQuery(Preconditions.checkNotNull(regexp,
                            "regexp may not be null"), caseInsensitive ? "i" : null);
            return this;
        }

        /**
         * Only returns analyses (filters out jobs).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #withClassJob()}.
         * </p>
         *
         * @return the same builder object
         */
        @SuppressWarnings("unchecked")
        public FindExecutionsRequestBuilder<DXAnalysis> withClassAnalysis() {
            Preconditions.checkState(this.classConstraint == null,
                    "Cannot specify class constraints more than once");
            this.classConstraint = "analysis";
            // This cast should be safe, since we hold no references of type T
            return (FindExecutionsRequestBuilder<DXAnalysis>) this;
        }

        /**
         * Only returns jobs (filters out analyses).
         *
         * <p>
         * This method may only be called once during the construction of a query, and is mutually
         * exclusive with {@link #withClassAnalysis()}.
         * </p>
         *
         * @return the same builder object
         */
        @SuppressWarnings("unchecked")
        public FindExecutionsRequestBuilder<DXJob> withClassJob() {
            Preconditions.checkState(this.classConstraint == null,
                    "Cannot specify class constraints more than once");
            this.classConstraint = "job";
            // This cast should be safe, since we hold no references of type T
            return (FindExecutionsRequestBuilder<DXJob>) this;
        }

        /**
         * Only return executions with the specified executable.
         *
         * @param executable executable
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withExecutable(DXExecutable<?> executable) {
            Preconditions.checkState(this.executable == null,
                    "Cannot specify withExecutable more than once");
            this.executable =
                    Preconditions.checkNotNull(executable, "executable may not be null").getId();
            return this;
        }

        /**
         * Only returns executions whose IDs match one of the specified executions.
         *
         * <p>
         * When used in combination with {@link #includeDescribeOutput()} you can use this to
         * "bulk describe" a large number of executions.
         * </p>
         *
         * @param executions collection of executions
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withIdsIn(
                Collection<? extends DXExecution> executions) {
            Preconditions.checkState(this.id == null, "Cannot call withIdsIn more than once");
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (DXExecution d : executions) {
                builder.add(d.getId());
            }
            this.id = builder.build();
            return this;
        }

        /**
         * Only return executions with the specified origin job.
         *
         * @param originJob origin job
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withOriginJob(DXJob originJob) {
            // TODO: API allows specifying more than one origin job
            Preconditions.checkState(this.originJob == null,
                    "Cannot specify withOriginJob more than once");
            this.originJob =
                    Preconditions.checkNotNull(originJob, "originJob may not be null").getId();
            return this;
        }

        /**
         * Only return executions with the specified parent analysis.
         *
         * @param parentAnalysis parent analysis
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withParentAnalysis(DXAnalysis parentAnalysis) {
            Preconditions.checkState(this.parentAnalysis == null,
                    "Cannot specify withParentAnalysis more than once");
            this.parentAnalysis =
                    Preconditions.checkNotNull(parentAnalysis, "parentAnalysis may not be null")
                            .getId();
            return this;
        }

        /**
         * Only return executions with the specified parent job.
         *
         * @param parentJob parent job
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withParentJob(DXJob parentJob) {
            Preconditions.checkState(this.parentJob == null,
                    "Cannot specify withParentJob more than once");
            this.parentJob =
                    Preconditions.checkNotNull(parentJob, "parentJob may not be null").getId();
            return this;
        }

        /**
         * Only returns executions matching the specified properties query.
         *
         * @param propertiesQuery properties query
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withProperties(PropertiesQuery propertiesQuery) {
            // Multiple calls to withProperty are allowed here for backwards compatiblity (this was
            // how you could query on multiple properties before PropertiesQuery.allOf was
            // introduced). However, other fields don't support analogous functionality and it could
            // be removed some time in the future.
            this.properties.add(Preconditions.checkNotNull(propertiesQuery));
            return this;
        }

        /**
         * Only returns executions where the specified property is present.
         *
         * <p>
         * To specify a complex query on the properties, use
         * {@link #withProperties(DXSearch.PropertiesQuery)}.
         * </p>
         *
         * @param propertyKey property key that must be present
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withProperty(String propertyKey) {
            return withProperties(PropertiesQuery.withKey(propertyKey));
        }

        /**
         * Only returns executions where the specified property has the specified value.
         *
         * <p>
         * To specify a complex query on the properties, use
         * {@link #withProperties(DXSearch.PropertiesQuery)}.
         * </p>
         *
         * @param propertyKey property key
         * @param propertyValue property value
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withProperty(String propertyKey, String propertyValue) {
            return withProperties(PropertiesQuery.withKeyAndValue(propertyKey, propertyValue));
        }

        /**
         * Only return executions with the specified root execution.
         *
         * @param rootExecution root execution
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withRootExecution(DXExecution rootExecution) {
            // TODO: API allows specifying more than one root execution
            Preconditions.checkState(this.rootExecution == null,
                    "Cannot specify withRootExecution more than once");
            this.rootExecution =
                    Preconditions.checkNotNull(rootExecution, "rootExecution may not be null")
                            .getId();
            return this;
        }

        /**
         * Only returns executions in one of the specified analysis states. If used in combination
         * with {@link #withState(JobState...)}, the union of the selected job and analysis states
         * is allowed.
         *
         * <p>
         * Note that it is possible for such a query to select jobs if the jobs are in a state that
         * has the same name as an analysis state, e.g. DONE.
         * </p>
         *
         * @param states analysis states
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withState(AnalysisState... states) {
            Preconditions.checkState(this.analysisStates.size() == 0,
                    "Cannot specify withState(JobState) more than once");
            Preconditions.checkArgument(states.length > 0, "At least one state must be provided");
            this.analysisStates.addAll(ImmutableList.copyOf(states));
            return this;
        }

        /**
         * Only returns executions in one of the specified job states. If used in combination with
         * {@link #withState(AnalysisState...)}, the union of the selected job and analysis states
         * is allowed.
         *
         * <p>
         * Note that it is possible for such a query to select analyses if the analyses are in a
         * state that has the same name as a job state, e.g. DONE.
         * </p>
         *
         * @param states job states
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withState(JobState... states) {
            Preconditions.checkState(this.jobStates.size() == 0,
                    "Cannot specify withState(JobState) more than once");
            Preconditions.checkArgument(states.length > 0, "At least one state must be provided");
            this.jobStates.addAll(ImmutableList.copyOf(states));
            return this;
        }

        /**
         * Only returns executions with the specified tag.
         *
         * <p>
         * To specify a complex query on the tags, use {@link #withTags(DXSearch.TagsQuery)}.
         * </p>
         *
         * @param tag String containing a tag
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withTag(String tag) {
            Preconditions.checkState(this.tags == null, "Cannot specify withTag* more than once");
            this.tags = TagsQuery.of(Preconditions.checkNotNull(tag, "tag may not be null"));
            return this;
        }

        /**
         * Only returns executions matching the specified tags query.
         *
         * @param tagsQuery tags query
         *
         * @return the same builder object
         */
        public FindExecutionsRequestBuilder<T> withTags(TagsQuery tagsQuery) {
            Preconditions.checkState(this.tags == null, "Cannot specify withTag* more than once");
            this.tags = Preconditions.checkNotNull(tagsQuery, "tagsQuery may not be null");
            return this;
        }

    }

    /**
     * Deserialized output from the /system/findExecutions route.
     */
    @VisibleForTesting
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class FindExecutionsResponse {

        @JsonIgnoreProperties(ignoreUnknown = true)
        private static class Entry {
            @JsonProperty
            private String id;
            @JsonProperty
            private JsonNode describe;
        }

        @JsonProperty
        private List<Entry> results;

        @JsonProperty
        private JsonNode next;

    }

    /**
     * The set of executions that matched a {@code findExecutions} query.
     *
     * <p>
     * This class paginates through the results as necessary to return the full result set.
     * </p>
     *
     * @param <T> execution class that will be returned from the query
     */
    public static class FindExecutionsResult<T extends DXExecution> extends ObjectProducerImpl<T> {

        /**
         * Wrapper from the findExecutions result page class to the high-level interface
         * FindResultPage.
         */
        private class FindExecutionsResultPage implements FindResultPage<T> {

            private final FindExecutionsResponse response;

            public FindExecutionsResultPage(FindExecutionsResponse response) {
                this.response = response;
            }

            @Override
            public T get(int index) {
                return getExecutionInstanceFromResult(response.results.get(index));
            }

            @Override
            public boolean hasNextPage() {
                return response.next != null;
            }

            @Override
            public int size() {
                return response.results.size();
            }

        }

        /**
         * Iterator implementation for findExecutions results.
         */
        private class ResultIterator
                extends
                PaginatingFindResultIterator<T, FindExecutionsRequest, FindExecutionsResultPage> {

            public ResultIterator() {
                super(baseQuery);
            }

            @Override
            public FindExecutionsRequest getNextQuery(FindExecutionsRequest query,
                    FindExecutionsResultPage currentResultPage) {
                return new FindExecutionsRequest(query, currentResultPage.response.next, pageSize);
            }

            @Override
            public FindExecutionsResultPage issueQuery(FindExecutionsRequest query) {
                return new FindExecutionsResultPage(DXAPI.systemFindExecutions(query,
                        FindExecutionsResponse.class, env));
            }
        }

        private final FindExecutionsRequest baseQuery;
        private final String classConstraint;
        private final DXEnvironment env;

        // Number of results to fetch with each API call, or null to use the default
        private final Integer pageSize;

        /**
         * Initializes this result set object with the default (API server-provided) page size.
         */
        private FindExecutionsResult(FindExecutionsRequest requestHash, String classConstraint,
                DXEnvironment env) {
            this.baseQuery = requestHash;
            this.classConstraint = classConstraint;
            this.env = env;

            this.pageSize = null;
        }

        /**
         * Initializes this result set object with the specified page size.
         */
        private FindExecutionsResult(FindExecutionsRequest requestHash, String classConstraint,
                DXEnvironment env, int pageSize) {
            this.baseQuery = requestHash;
            this.classConstraint = classConstraint;
            this.env = env;

            this.pageSize = pageSize;
        }

        @SuppressWarnings("unchecked")
        private T getExecutionInstanceFromResult(FindExecutionsResponse.Entry e) {
            DXExecution execution;
            if (e.describe != null) {
                execution = DXExecution.getInstanceWithCachedDescribe(e.id, env, e.describe);
            } else {
                execution = DXExecution.getInstanceWithEnvironment(e.id, env);
            }

            if (classConstraint != null) {
                if (!execution.getId().startsWith(classConstraint + "-")) {
                    throw new IllegalStateException("Expected all results to be of type "
                            + classConstraint + " but received an object with ID "
                            + execution.getId());
                }
            }

            // This is an unchecked cast, but the callers of this class
            // should have set T appropriately so that it agrees with
            // the class constraint (if any). If something goes wrong
            // here, either that code is incorrect or the API server
            // has returned incorrect results.
            return (T) execution;
        }

        @Override
        public Iterator<T> iterator() {
            return new ResultIterator();
        }

    }

    /**
     * Encapsulates a single result page (of generic type) for a find route.
     *
     * @param <T> Type of result to be returned e.g. DXDataObject for findDataObjects
     */
    private static interface FindResultPage<T> {
        /**
         * Returns the specified result from the current page.
         *
         * @param index index of result to obtain
         *
         * @return the requested result
         */
        public T get(int index);

        /**
         * Returns true if the next page exists.
         *
         * @return whether there are more results
         */
        public boolean hasNextPage();

        /**
         * Returns the number of results in the current page.
         *
         * @return size of the current page
         */
        public int size();
    }

    /**
     * Query on the name of an object (for finding data objects, executions, or apps).
     */
    private abstract static class NameQuery {

        /**
         * Query for objects where the name matches a particular string exactly.
         */
        private static class ExactNameQuery extends NameQuery {
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

        /**
         * Query for objects where the name matches a particular glob.
         */
        private static class GlobNameQuery extends NameQuery {
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

        /**
         * Query for objects where the name matches a particular regular expression.
         */
        private static class RegexpNameQuery extends NameQuery {
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
    }

    /**
     * Interface that provides streaming or buffered access to a sequence of {@code DXObjects}.
     *
     * @param <T> type of object to be returned
     */
    public static interface ObjectProducer<T extends DXObject> extends Iterable<T> {
        /**
         * Returns a list of the matching items.
         *
         * @return List of matching items
         */
        public List<T> asList();
    }

    /**
     * Default implementation of {@link ObjectProducer} that provides the
     * {@link ObjectProducer#asList()} method once you've implemented streaming iteration.
     *
     * @param <T> type of result to be returned
     */
    private static abstract class ObjectProducerImpl<T extends DXObject>
            implements
                ObjectProducer<T> {
        @Override
        public List<T> asList() {
            return ImmutableList.copyOf(this);
        }
    }

    /**
     * Encapsulates the strategy for paginating across find results.
     *
     * <p>
     * Individual subclasses (associated with each distinct "find" route) should override the
     * methods and provide an implementation of P, the result page class.
     * </p>
     *
     * @param <T> Type of result to be returned e.g. DXDataObject for findDataObjects
     * @param <Q> Type of query to be issued, e.g. FindDataObjectsRequest
     * @param <P> Class encapsulating a single result page (the result of a query of type Q)
     */
    private static abstract class PaginatingFindResultIterator<T, Q, P extends FindResultPage<T>>
            implements
                Iterator<T> {
        private Q query;
        private P currentPage;
        private int nextResultIndex = 0;

        /**
         * Initializes the iterator with the specified initial query.
         *
         * @param initialQuery the first query to execute (typically without the "starting"
         *        parameter set)
         */
        private PaginatingFindResultIterator(Q initialQuery) {
            this.query = initialQuery;
            this.currentPage = issueQuery(initialQuery);
        }

        /**
         * Ensures that the next element is loaded (if it exists).
         *
         * <p>
         * Postcondition: either findProjectsResponse.results.get(nextResultIndex) points to the
         * next valid result, or nextResultIndex is equal to findProjectsResponse.results.size() and
         * there are no results left.
         * </p>
         */
        private void ensureNextElementAvailable() {
            // If two successive calls are made to this method with no intervening mutators, the
            // second call should be very fast.
            if (nextResultIndex < currentPage.size()) {
                return;
            }
            if (!currentPage.hasNextPage()) {
                return;
            }
            // Reached the end of the previous page. Load a new page.
            query = getNextQuery(query, currentPage);
            currentPage = issueQuery(query);
            nextResultIndex = 0;
        }

        /**
         * Returns a query that can be used to obtain the next page of results. In general this
         * query can be obtained by taking the previous query and setting its "starting" field to
         * the "next" value from the query results page.
         *
         * @param query current page's query
         * @param currentResultPage the most recently returned page
         *
         * @return query for the next page
         */
        public abstract Q getNextQuery(Q query, P currentResultPage);

        @Override
        public boolean hasNext() {
            ensureNextElementAvailable();
            return nextResultIndex < currentPage.size();
        }

        /**
         * Issues the specified query and returns the next page of results.
         *
         * @param query query to issue
         *
         * @return a page of results
         */
        public abstract P issueQuery(Q query);

        @Override
        public T next() {
            ensureNextElementAvailable();
            return currentPage.get(nextResultIndex++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A query for objects (data objects, executions, or projects) with specified properties.
     */
    public static abstract class PropertiesQuery {

        private static class CompoundPropertiesQuery extends PropertiesQuery {
            private final String operator;
            private final List<PropertiesQuery> operands;

            public CompoundPropertiesQuery(String operator, List<PropertiesQuery> operands) {
                this.operator = operator;
                this.operands = ImmutableList.copyOf(operands);
            }

            @JsonValue
            protected JsonNode getValue() {
                return serializeCompoundQuery(this.operator, this.operands);
            }
        }

        private static class SimplePropertiesQuery extends PropertiesQuery {
            private final String propertyKey;
            private final String propertyValue;

            public SimplePropertiesQuery(String key) {
                this.propertyKey = Preconditions.checkNotNull(key);
                this.propertyValue = null;
            }

            public SimplePropertiesQuery(String key, String value) {
                this.propertyKey = Preconditions.checkNotNull(key);
                this.propertyValue = Preconditions.checkNotNull(value);
            }

            @SuppressWarnings("unused")
            @JsonValue
            protected Object getValue() {
                if (propertyValue == null) {
                    return DXJSON.getObjectBuilder().put(propertyKey, true).build();
                }
                return DXJSON.getObjectBuilder().put(propertyKey, propertyValue).build();
            }
        }

        /**
         * A query that must match all of the property queries in the provided list.
         *
         * @param propertiesQueries list of queries, all of which must be matched
         *
         * @return query
         */
        public static PropertiesQuery allOf(List<PropertiesQuery> propertiesQueries) {
            return new CompoundPropertiesQuery("$and", propertiesQueries);
        }

        /**
         * A query that must match all of the specified property queries recursively.
         *
         * @param propertiesQueries queries, all of which must be matched
         *
         * @return query
         */
        public static PropertiesQuery allOf(PropertiesQuery... propertiesQueries) {
            return PropertiesQuery.allOf(ImmutableList.copyOf(propertiesQueries));
        }

        /**
         * A query that matches any of the property queries in the provided list.
         *
         * @param propertiesQueries list of queries, at least one of which must be matched
         *
         * @return query
         */
        public static PropertiesQuery anyOf(List<PropertiesQuery> propertiesQueries) {
            return new CompoundPropertiesQuery("$or", propertiesQueries);
        }

        /**
         * A query that matches any of the specified property queries recursively.
         *
         * @param propertiesQueries queries, at least one of which must be matched
         *
         * @return query
         */
        public static PropertiesQuery anyOf(PropertiesQuery... propertiesQueries) {
            return PropertiesQuery.anyOf(ImmutableList.copyOf(propertiesQueries));
        }

        /**
         * A query that the specified property key is present.
         *
         * @param propertyKey property key that must be present
         *
         * @return query
         */
        public static PropertiesQuery withKey(String propertyKey) {
            // TODO: some more overloads might be nice here, so you could write something like
            // PropertiesQuery.withKeys("a", "b") instead of
            // PropertiesQuery.allOf(PropertiesQuery.withKey("a"), PropertiesQuery.withKey("b"))
            return new SimplePropertiesQuery(propertyKey);
        }

        /**
         * A query that the specified property key has the specified property value.
         *
         * @param propertyKey property key
         * @param propertyValue property value
         *
         * @return query
         */
        public static PropertiesQuery withKeyAndValue(String propertyKey, String propertyValue) {
            // TODO: some more overloads might be nice here, so you could write something like
            // PropertiesQuery.withKeysAndValues("a", "a1", "b", "b1") instead of
            // PropertiesQuery.allOf(PropertiesQuery.withKeyAndValue("a", "a1"),
            // PropertiesQuery.withKey("b", "b1"))
            return new SimplePropertiesQuery(propertyKey, propertyValue);
        }

        private PropertiesQuery() {
            // Do not allow subclassing except by the implementations provided here
        }

    }


    /**
     * A query for objects (data objects, executions, or projects) with specified tags.
     */
    public static abstract class TagsQuery {

        private static class CompoundTagsQuery extends TagsQuery {
            private final String operator;
            private final List<TagsQuery> operands;

            public CompoundTagsQuery(String operator, List<TagsQuery> operands) {
                this.operator = operator;
                this.operands = ImmutableList.copyOf(operands);
            }

            @JsonValue
            protected JsonNode getValue() {
                return serializeCompoundQuery(this.operator, this.operands);
            }
        }

        private static class SimpleTagsQuery extends TagsQuery {
            private final String tag;

            public SimpleTagsQuery(String tag) {
                this.tag = Preconditions.checkNotNull(tag);
            }

            @SuppressWarnings("unused")
            @JsonValue
            protected String getValue() {
                return this.tag;
            }
        }

        /**
         * A query that must match all of the tag queries in the provided list.
         *
         * @param tagsQueries list of queries, all of which must be matched
         *
         * @return query
         */
        public static TagsQuery allOf(List<TagsQuery> tagsQueries) {
            return new CompoundTagsQuery("$and", tagsQueries);
        }

        /**
         * A query that must match all of the specified tags.
         *
         * @param tags Strings containing tags, all of which must be matched
         *
         * @return query
         */
        public static TagsQuery allOf(String... tags) {
            List<TagsQuery> tagsQueries = Lists.newArrayList();
            for (String tag : tags) {
                tagsQueries.add(TagsQuery.of(tag));
            }
            return TagsQuery.allOf(tagsQueries);
        }

        /**
         * A query that must match all of the specified tag queries recursively.
         *
         * @param tagsQueries queries, all of which must be matched
         *
         * @return query
         */
        public static TagsQuery allOf(TagsQuery... tagsQueries) {
            return TagsQuery.allOf(ImmutableList.copyOf(tagsQueries));
        }

        /**
         * A query that matches any of the tag queries in the provided list.
         *
         * @param tagsQueries list of queries, at least one of which must be matched
         *
         * @return query
         */
        public static TagsQuery anyOf(List<TagsQuery> tagsQueries) {
            return new CompoundTagsQuery("$or", tagsQueries);
        }

        /**
         * A query that matches any of the specified tags.
         *
         * @param tags Strings containing tags, at least one of which must be matched
         *
         * @return query
         */
        public static TagsQuery anyOf(String... tags) {
            List<TagsQuery> tagsQueries = Lists.newArrayList();
            for (String tag : tags) {
                tagsQueries.add(TagsQuery.of(tag));
            }
            return TagsQuery.anyOf(tagsQueries);
        }

        /**
         * A query that matches any of the specified tag queries recursively.
         *
         * @param tagsQueries queries, at least one of which must be matched
         *
         * @return query
         */
        public static TagsQuery anyOf(TagsQuery... tagsQueries) {
            return TagsQuery.anyOf(ImmutableList.copyOf(tagsQueries));
        }

        /**
         * A query that matches the specified tag.
         *
         * @param tag String containing tag to match
         *
         * @return query
         */
        public static TagsQuery of(String tag) {
            return new SimpleTagsQuery(tag);
        }

        private TagsQuery() {
            // Do not allow subclassing except by the implementations provided here
        }

    }

    /**
     * Query for a time (e.g. creation or modification time) falling in some interval (either
     * bounded on both sides, or bounded on one side only).
     */
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

    /**
     * A query for data objects with specified types.
     */
    public static abstract class TypeQuery {

        private static class CompoundTypeQuery extends TypeQuery {
            private final String operator;
            private final List<TypeQuery> operands;

            public CompoundTypeQuery(String operator, List<TypeQuery> operands) {
                this.operator = operator;
                this.operands = ImmutableList.copyOf(operands);
            }

            @JsonValue
            protected JsonNode getValue() {
                return serializeCompoundQuery(this.operator, this.operands);
            }

        }

        private static class SimpleTypeQuery extends TypeQuery {
            private final String type;

            public SimpleTypeQuery(String type) {
                this.type = Preconditions.checkNotNull(type);
            }

            @JsonValue
            protected String getValue() {
                return this.type;
            }
        }

        /**
         * A query that must match all of the type queries in the provided list.
         *
         * @param typeQueries list of queries, all of which must be matched
         *
         * @return query
         */
        public static TypeQuery allOf(List<TypeQuery> typeQueries) {
            return new CompoundTypeQuery("$and", typeQueries);
        }

        /**
         * A query that must match all of the specified types.
         *
         * @param types Strings containing types, all of which must be matched
         *
         * @return query
         */
        public static TypeQuery allOf(String... types) {
            List<TypeQuery> typeQueries = Lists.newArrayList();
            for (String type : types) {
                typeQueries.add(TypeQuery.of(type));
            }
            return TypeQuery.allOf(typeQueries);
        }

        /**
         * A query that must match all of the specified type queries recursively.
         *
         * @param typeQueries queries, all of which must be matched
         *
         * @return query
         */
        public static TypeQuery allOf(TypeQuery... typeQueries) {
            return TypeQuery.allOf(ImmutableList.copyOf(typeQueries));
        }

        /**
         * A query that matches any of the type queries in the provided list.
         *
         * @param typeQueries list of queries, at least one of which must be matched
         *
         * @return query
         */
        public static TypeQuery anyOf(List<TypeQuery> typeQueries) {
            return new CompoundTypeQuery("$or", typeQueries);
        }

        /**
         * A query that matches any of the specified types.
         *
         * @param types Strings containing types, at least one of which must be matched
         *
         * @return query
         */
        public static TypeQuery anyOf(String... types) {
            List<TypeQuery> typeQueries = Lists.newArrayList();
            for (String type : types) {
                typeQueries.add(TypeQuery.of(type));
            }
            return TypeQuery.anyOf(typeQueries);
        }

        /**
         * A query that matches any of the specified type queries recursively.
         *
         * @param typeQueries queries, at least one of which must be matched
         *
         * @return query
         */
        public static TypeQuery anyOf(TypeQuery... typeQueries) {
            return TypeQuery.anyOf(ImmutableList.copyOf(typeQueries));
        }

        /**
         * A query that matches the specified type.
         *
         * @param type String containing type to match
         *
         * @return query
         */
        public static TypeQuery of(String type) {
            return new SimpleTypeQuery(type);
        }

        private TypeQuery() {
            // Do not allow subclassing except by the implementations provided here
        }

    }

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
     * Returns a builder object for finding executions (jobs or analyses) that match certain
     * criteria.
     *
     * @return a newly initialized builder object
     */
    public static FindExecutionsRequestBuilder<DXExecution> findExecutions() {
        return new FindExecutionsRequestBuilder<DXExecution>();
    }

    /**
     * Returns a builder object for finding executions that match certain criteria, using the
     * specified environment.
     *
     * @param env environment specifying API server parameters for issuing the query; the
     *        environment will be propagated into objects that are subsequently returned
     *
     * @return a newly initialized builder object
     */
    public static FindExecutionsRequestBuilder<DXExecution> findExecutionsWithEnvironment(
            DXEnvironment env) {
        return new FindExecutionsRequestBuilder<DXExecution>(env);
    }

    /**
     * Returns a builder object for finding jobs that match certain criteria.
     *
     * <p>
     * This is equivalent to <code>findExecutions().withClassJob()</code>.
     * </p>
     *
     * @deprecated Use {@link DXSearch#findExecutions()} in conjunction with
     *             {@link FindExecutionsRequestBuilder#withClassJob()} instead.
     *
     * @return a newly initialized builder object
     */
    @Deprecated
    public static FindExecutionsRequestBuilder<DXJob> findJobs() {
        return new FindExecutionsRequestBuilder<DXExecution>().withClassJob();
    }

    /**
     * Returns a builder object for finding jobs that match certain criteria, using the specified
     * environment.
     *
     * <p>
     * This is equivalent to <code>findExecutionsWithEnvironment(env).withClassJob()</code>.
     * </p>
     *
     * @deprecated Use {@link DXSearch#findExecutionsWithEnvironment(DXEnvironment)} in conjunction
     *             with {@link FindExecutionsRequestBuilder#withClassJob()} instead.
     *
     * @param env environment specifying API server parameters for issuing the query; the
     *        environment will be propagated into objects that are subsequently returned
     *
     * @return a newly initialized builder object
     */
    @Deprecated
    public static FindExecutionsRequestBuilder<DXJob> findJobsWithEnvironment(DXEnvironment env) {
        return new FindExecutionsRequestBuilder<DXExecution>(env).withClassJob();
    }

    // Prevent this utility class from being instantiated.
    private DXSearch() {}

    // Serializes a query to the form
    //
    //   {operator: [operand1, operand2, ...]}
    //
    // where operator is typically either "$and" or "$or"
    private static JsonNode serializeCompoundQuery(String operator, List<? extends Object> operands) {
        List<JsonNode> transformedArgs = Lists.newArrayList();
        for (Object operand : operands) {
            transformedArgs.add(MAPPER.valueToTree(operand));
        }
        return DXJSON.getObjectBuilder()
                .put(operator, DXJSON.getArrayBuilder().addAll(transformedArgs).build()).build();
    }

}
