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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base class for all data object classes in the DNAnexus Platform.
 */
public abstract class DXDataObject extends DXObject {

    /**
     * Request to /class-xxxx/{add,remove}Tags
     */
    @JsonInclude(Include.NON_NULL)
    private static class AddOrRemoveTagsRequest {
        @SuppressWarnings("unused")
        @JsonProperty("project")
        private String projectId;
        @SuppressWarnings("unused")
        @JsonProperty
        private List<String> tags;

        private AddOrRemoveTagsRequest(String projectId, List<String> tags) {
            this.projectId = projectId;
            this.tags = ImmutableList.copyOf(tags);
        }
    }

    /**
     * Request to /class-xxxx/{add,remove}Types
     */
    @JsonInclude(Include.NON_NULL)
    private static class AddOrRemoveTypesRequest {
        @SuppressWarnings("unused")
        @JsonProperty
        private List<String> types;

        private AddOrRemoveTypesRequest(List<String> types) {
            this.types = ImmutableList.copyOf(types);
        }
    }

    /**
     * Abstract builder class for creating a new data object of class {@code U}.
     *
     * @param <T> the builder type subclass
     * @param <U> class of data object to be created
     */
    protected static abstract class Builder<T extends Builder<T, U>, U extends DXDataObject> {

        /**
         * Deserializes the response to a {@code /class-xxxx/new} API call and returns the ID of the
         * newly created object.
         *
         * @return DNAnexus object ID
         */
        protected static String getNewObjectId(JsonNode responseJson) {
            return DXJSON.safeTreeToValue(responseJson, DataObjectNewResponse.class).id;
        }

        protected DXContainer project = null;
        protected String name = null;
        protected String folder = null;
        protected Boolean createParents = null;
        protected List<String> tags = null;
        protected List<String> types = null;
        protected JsonNode details = null;
        protected Boolean hidden = null;

        protected ImmutableMap.Builder<String, String> properties;

        protected final DXEnvironment env;

        protected Builder() {
            this.env = DXEnvironment.create();
        }

        protected Builder(DXEnvironment env) {
            this.env = env;
        }

        /**
         * Adds the specified tags to the newly created data object.
         *
         * @param tags tags to add
         *
         * @return the same {@code Builder} object
         */
        public T addTags(Collection<String> tags) {
            if (this.tags == null) {
                this.tags = Lists.newArrayList();
            }
            this.tags.addAll(Preconditions.checkNotNull(tags, "tags may not be null"));
            return getThisInstance();
        }

        /**
         * Adds the specified types to the newly created data object.
         *
         * @param types types to add
         *
         * @return the same {@code Builder} object
         */
        public T addTypes(Collection<String> types) {
            if (this.types == null) {
                this.types = Lists.newArrayList();
            }
            this.types.addAll(Preconditions.checkNotNull(types, "types may not be null"));
            return getThisInstance();
        }

        /**
         * Creates the new data object.
         *
         * @return a {@code DXDataObject} corresponding to the newly created object
         */
        public abstract U build();

        /**
         * Ensures that the project was either explicitly set or that the environment specifies a
         * workspace.
         */
        protected void checkAndFixParameters() {
            if (this.project == null) {
                this.project = this.env.getWorkspace();
            }
            Preconditions
                    .checkState(this.project != null,
                            "setProject must be specified if the environment does not have a workspace set");
        }

        /**
         * Returns the builder object.
         *
         * <p>
         * This abstract method is implemented by the Builder methods so that common methods can get
         * an instance of the subclass for chaining purposes.
         * </p>
         *
         * @return the same object
         */
        protected abstract T getThisInstance();

        /**
         * Sets the specified properties on the newly created data object.
         *
         * @param properties Map containing non-null keys and values which will be set as property
         *        keys and values respectively
         *
         * @return the same {@code Builder} object
         */
        public T putAllProperties(Map<String, String> properties) {
            for (Map.Entry<String, String> e : properties.entrySet()) {
                putProperty(e.getKey(), e.getValue());
            }
            return getThisInstance();
        }

        /**
         * Sets the specified property on the newly created data object.
         *
         * @param key property key to set
         * @param value property value to set
         *
         * @return the same {@code Builder} object
         */
        public T putProperty(String key, String value) {
            if (this.properties == null) {
                this.properties = ImmutableMap.builder();
            }
            this.properties.put(
                    Preconditions.checkNotNull(key, "Property key may not be null"),
                    Preconditions.checkNotNull(value, "Value for property " + key
                            + " may not be null"));
            return getThisInstance();
        }

        /**
         * Sets the details of the data object to be created.
         *
         * @param details an object whose JSON serialized form will be set as the details
         *
         * @return the same {@code Builder} object
         */
        public T setDetails(Object details) {
            Preconditions.checkState(this.details == null, "Cannot call setDetails more than once");
            this.details =
                    MAPPER.valueToTree(Preconditions.checkNotNull(details,
                            "details may not be null"));
            return getThisInstance();
        }

        /**
         * Sets the folder in which the data object will be created.
         *
         * @param folder full path to destination folder (a String starting with {@code "/"})
         *
         * @return the same {@code Builder} object
         */
        public T setFolder(String folder) {
            Preconditions.checkState(this.folder == null, "Cannot call setFolder more than once");
            this.folder = Preconditions.checkNotNull(folder, "folder may not be null");
            return getThisInstance();
        }

        /**
         * Sets the folder in which the data object will be created, optionally specifying that the
         * folder and its parents should be created if necessary.
         *
         * @param folder full path to destination folder (a String starting with {@code "/"})
         * @param createParents if true, the folder will be created if it doesn't exist
         *
         * @return the same {@code Builder} object
         */
        public T setFolder(String folder, boolean createParents) {
            Preconditions.checkState(this.folder == null, "Cannot call setFolder more than once");
            this.folder = Preconditions.checkNotNull(folder, "folder may not be null");
            this.createParents = createParents;
            return getThisInstance();
        }

        /**
         * Sets the name of the newly created data object.
         *
         * @param name name to set
         *
         * @return the same {@code Builder} object
         */
        public T setName(String name) {
            Preconditions.checkState(this.name == null, "Cannot call setName more than once");
            this.name = Preconditions.checkNotNull(name, "name may not be null");
            return getThisInstance();
        }

        /**
         * Sets the project or container where the new data object will be created.
         *
         * @param project {@code DXProject} or {@code DXContainer}
         *
         * @return the same {@code Builder} object
         */
        public T setProject(DXContainer project) {
            Preconditions.checkState(this.project == null, "Cannot call setProject more than once");
            this.project = Preconditions.checkNotNull(project, "project may not be null");
            return getThisInstance();
        }

        /**
         * Sets the visibility of the new data object.
         *
         * @param visible if false, the object will be hidden
         *
         * @return the same {@code Builder} object
         */
        public T setVisibility(boolean visible) {
            Preconditions.checkState(this.hidden == null,
                    "Cannot call setVisibility more than once");
            this.hidden = !visible;
            return getThisInstance();
        }

    }

    /**
     * Request to /class/new
     */
    @JsonInclude(Include.NON_NULL)
    static class DataObjectNewRequest {
        @SuppressWarnings("unused")
        @JsonProperty("project")
        private String projectId;
        @SuppressWarnings("unused")
        @JsonProperty
        private String name;
        @SuppressWarnings("unused")
        @JsonProperty
        private String folder;
        @SuppressWarnings("unused")
        @JsonProperty("parents")
        private Boolean createParents;
        @SuppressWarnings("unused")
        @JsonProperty
        private Boolean hidden;
        @SuppressWarnings("unused")
        @JsonProperty
        private List<String> types;
        @SuppressWarnings("unused")
        @JsonProperty
        private JsonNode details;
        @SuppressWarnings("unused")
        @JsonProperty
        private List<String> tags;
        @SuppressWarnings("unused")
        @JsonProperty
        private Map<String, String> properties;

        protected <T extends Builder<T, U>, U extends DXDataObject> DataObjectNewRequest(
                Builder<T, U> builder) {
            this.projectId = builder.project.getId();
            this.name = builder.name;
            this.folder = builder.folder;
            this.createParents = builder.createParents;
            this.tags = builder.tags;
            this.types = builder.types;
            this.details = builder.details;
            this.hidden = builder.hidden;

            // If no properties set, omit the field entirely rather than send an empty hash
            if (builder.properties != null) {
                this.properties = builder.properties.build();
            }
        }
    }

    /**
     * Response from /class/new
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class DataObjectNewResponse {
        @JsonProperty
        private String id;
    }

    /**
     * Contains metadata for a data object (fields common to all data objects). All accessors
     * reflect the state of the data object at the time that this object was created.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Describe {
        @JsonProperty
        private String project;
        @SuppressWarnings("unused")
        @JsonProperty
        private String id;
        @JsonProperty
        private List<String> types;
        @JsonProperty
        private DataObjectState state;
        @JsonProperty
        private Boolean hidden;
        @JsonProperty
        private String name;
        @JsonProperty
        private String folder;
        @JsonProperty
        private List<String> tags;
        @JsonProperty
        private JsonNode details;
        @JsonProperty
        private Map<String, String> properties;
        @JsonProperty
        private Long created;
        @JsonProperty
        private Long modified;

        /**
         * Creates a {@code Describe} object with all empty metadata.
         */
        protected Describe() {}

        // TODO: links, sponsored, createdBy

        /**
         * Returns the creation date of the object.
         *
         * @return creation date
         */
        public Date getCreationDate() {
            Preconditions
                    .checkState(this.created != null,
                            "creation time is not accessible because it was not retrieved with the describe call");
            return new Date(this.created);
        }

        /**
         * Returns the details of the object. This field may not be available unless
         * {@link DXDataObject#describe(DescribeOptions)} (or
         * {@link DXSearch.FindDataObjectsRequestBuilder#includeDescribeOutput(DXDataObject.DescribeOptions)}
         * ) was called with {@link DescribeOptions#withDetails()} set.
         *
         * @param valueType class to deserialize as
         *
         * @return the object's details
         *
         * @throws IllegalStateException if details were not retrieved with the describe call
         */
        public <T> T getDetails(Class<T> valueType) {
            Preconditions
                    .checkState(this.details != null,
                            "details are not accessible because they were not retrieved with the describe call");
            return DXJSON.safeTreeToValue(this.details, valueType);
        }

        /**
         * Returns the folder that contains the object.
         *
         * @return full path to the containing folder (a String starting with {@code "/"})
         */
        public String getFolder() {
            Preconditions.checkState(this.folder != null,
                    "folder is not accessible because it was not retrieved with the describe call");
            return this.folder;
        }

        /**
         * Returns the last modification date of the object.
         *
         * @return modification date
         */
        public Date getModificationDate() {
            Preconditions
                    .checkState(this.modified != null,
                            "modification time is not accessible because it was not retrieved with the describe call");
            return new Date(this.modified);
        }

        /**
         * Returns the name of the object.
         *
         * @return the object's name
         */
        public String getName() {
            Preconditions.checkState(this.name != null,
                    "name is not accessible because it was not retrieved with the describe call");
            return this.name;
        }

        /**
         * Returns the project or container from which user-provided metadata was retrieved.
         *
         * @return {@code DXProject} or {@code DXContainer}
         */
        public DXContainer getProject() {
            Preconditions
                    .checkState(this.project != null,
                            "project is not accessible because it was not retrieved with the describe call");
            return DXContainer.getInstance(this.project);
        }

        /**
         * Returns the properties associated with the object. This field may not be available unless
         * {@link DXDataObject#describe(DescribeOptions)} (or
         * {@link DXSearch.FindDataObjectsRequestBuilder#includeDescribeOutput(DXDataObject.DescribeOptions)}
         * ) was called with {@link DescribeOptions#withProperties()} set.
         *
         * @return Map of property keys to property values
         *
         * @throws IllegalStateException if properties were not retrieved with the describe call
         */
        public Map<String, String> getProperties() {
            Preconditions
                    .checkState(this.properties != null,
                            "properties is not accessible because it was not retrieved with the describe call");
            return ImmutableMap.copyOf(this.properties);
        }

        /**
         * Returns the state of the object.
         *
         * @return a {@code DXObjectState} indicating the current state
         */
        public DataObjectState getState() {
            Preconditions.checkState(this.state != null,
                    "state is not accessible because it was not retrieved with the describe call");
            return this.state;
        }

        /**
         * Returns a list of tags associated with the object.
         *
         * @return List of tags
         */
        public List<String> getTags() {
            Preconditions.checkState(this.tags != null,
                    "tags is not accessible because it was not retrieved with the describe call");
            // TODO: here and elsewhere, avoid creating this ImmutableList multiple times if the
            // client requests it multiple times.
            return ImmutableList.copyOf(this.tags);
        }

        /**
         * Returns a list of types associated with the object.
         *
         * @return List of types
         */
        public List<String> getTypes() {
            Preconditions.checkState(this.types != null,
                    "types is not accessible because it was not retrieved with the describe call");
            return ImmutableList.copyOf(this.types);
        }

        /**
         * Returns whether the object is visible.
         *
         * @return true if the object is visible
         */
        public boolean isVisible() {
            Preconditions
                    .checkState(this.hidden != null,
                            "visibility is not accessible because it was not retrieved with the describe call");
            return !this.hidden;
        }

    }

    /**
     * Configuration options for a describe call on a data object ({@literal e.g.}
     * {@link DXDataObject#describe(DescribeOptions)}) to control what optional fields get returned
     * and what project to obtain project-specific metadata from.
     *
     * <p>
     * Examples:
     * </p>
     *
     * <pre>
     * DescribeOptions.get().inProject(proj).withDetails()<br>
     * DescribeOptions.get().withProperties()
     * </pre>
     */
    @JsonInclude(Include.NON_NULL)
    public static class DescribeOptions {
        /**
         * Returns a default instance of {@code DescribeOptions} that returns no optional fields and
         * selects the project arbitrarily.
         *
         * @return a newly initialized {@code DescribeOptions} object
         */
        public static DescribeOptions get() {
            return new DescribeOptions();
        }

        @JsonProperty("project")
        private final String projectId;
        @JsonProperty
        private final Boolean properties;
        @JsonProperty
        private final Boolean details;
        @JsonProperty
        private final Map<String, Boolean> fields;

        private DescribeOptions() {
            this(null, null, null, null);
        }

        private DescribeOptions(String projectId, Map<String, Boolean> fields, Boolean properties, Boolean details) {
            this.projectId = projectId;
            this.fields = fields;
            this.properties = properties;
            this.details = details;
        }

        /**
         * Returns a {@code DescribeOptions} that behaves like the current one, except that
         * project-specific metadata will be retrieved from the specified project or container.
         * Attempts to invoke accessors on the resulting {@link Describe} object corresponding to
         * fields that were not requested will throw {@link IllegalStateException}.
         *
         * @param project project or container from which to obtain project-specific metadata
         *
         * @return a new {@code DescribeOptions} object
         */
        public DescribeOptions inProject(DXContainer project) {
            return new DescribeOptions(project.getId(), this.fields, this.properties, this.details);
        }

        /**
         * Returns a {@code DescribeOptions} that behaves like the current one, except that only the
         * specified fields will be included in the result. Attempts to invoke accessors on the
         * resulting {@link Describe} object corresponding to fields that were not requested will
         * throw {@link IllegalStateException}.
         *
         * @param fieldNamesToInclude API fields to be included
         *
         * @return a new {@code DescribeOptions} object
         */
        public DescribeOptions withCustomFields(String... fieldNamesToInclude) {
            return this.withCustomFields(Lists.newArrayList(fieldNamesToInclude));
        }

        /**
         * Returns a {@code DescribeOptions} that behaves like the current one, except that only the
         * fields in the specified collection will be included in the result.
         *
         * @param fieldNamesToInclude collection of API fields to be included
         *
         * @return a new {@code DescribeOptions} object
         */
        public DescribeOptions withCustomFields(Collection<? extends String> fieldNamesToInclude) {
            Preconditions.checkNotNull(fieldNamesToInclude);
            Preconditions.checkState(this.properties == null,
                    "withProperties may not be used with fieldNamesToInclude");
            Preconditions.checkState(this.details == null, "withDetails may not be used with fieldNamesToInclude");
            ImmutableMap.Builder<String, Boolean> fieldMap = ImmutableMap.builder();
            for (String fieldNameToInclude : fieldNamesToInclude) {
                fieldMap.put(fieldNameToInclude, true);
            }
            return new DescribeOptions(this.projectId, fieldMap.build(), false, false);
        }

        /**
         * Returns a {@code DescribeOptions} that behaves like the current one, except that the
         * details field will be included in the result.
         *
         * @return a new {@code DescribeOptions} object
         */
        public DescribeOptions withDetails() {
            return new DescribeOptions(this.projectId, null, this.properties, true);
        }

        /**
         * Returns a {@code DescribeOptions} that behaves like the current one, except that the
         * properties field will be included in the result.
         *
         * @return a new {@code DescribeOptions} object
         */
        public DescribeOptions withProperties() {
            return new DescribeOptions(this.projectId, null, true, this.details);
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class RenameRequest {
        @SuppressWarnings("unused")
        @JsonProperty("project")
        private String projectId;
        @SuppressWarnings("unused")
        @JsonProperty
        private String name;

        private RenameRequest(String projectId, String name) {
            this.projectId = projectId;
            this.name = name;
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class SetPropertiesRequest {
        @SuppressWarnings("unused")
        @JsonProperty("project")
        private String projectId;
        @SuppressWarnings("unused")
        @JsonProperty
        private Map<String, String> properties;

        private SetPropertiesRequest(String projectId, Map<String, String> propertiesToSet,
                List<String> propertiesToUnset) {
            this.projectId = projectId;

            // We don't use ImmutableMap here because it doesn't support null values.
            Map<String, String> propertyMap = Maps.newHashMap();
            for (Map.Entry<String, String> e : propertiesToSet.entrySet()) {
                propertyMap.put(
                        Preconditions.checkNotNull(e.getKey(), "Property key may not be null"),
                        Preconditions.checkNotNull(e.getValue(),
                                "Property value for key " + e.getKey() + " may not be null"));
            }
            for (String propertyToUnset : propertiesToUnset) {
                propertyMap.put(propertyToUnset, null);
            }
            this.properties = Collections.unmodifiableMap(propertyMap);
        }
    }

    @JsonInclude(Include.NON_NULL)
    private static class SetVisibilityRequest {
        @SuppressWarnings("unused")
        @JsonProperty
        private boolean hidden;

        private SetVisibilityRequest(boolean hidden) {
            this.hidden = hidden;
        }
    }

    // Not sure how to do this (deserialization to a Map object instead of
    // a user-defined class) without bringing in this new ObjectReader
    private static final ObjectReader listProjectsReader = MAPPER
            .reader(new TypeReference<Map<String, AccessLevel>>() {
                // Empty body for Jackson's TypeReference
            });

    /**
     * Verifies that the specified map has the format of a DNAnexus link.
     *
     * @param value putative DNAnexus link
     */
    protected static void checkDXLinkFormat(Map<String, Object> value) {
        if (!value.containsKey("$dnanexus_link")) {
            throw new IllegalArgumentException(
                    "Object must contain a field $dnanexus_link to be deserialized");
        }
    }

    /**
     * Deserializes a DXDataObject from JSON containing a DNAnexus link.
     *
     * @param value JSON object map
     *
     * @return data object
     */
    @SuppressWarnings("unused")
    @JsonCreator
    private static DXDataObject create(Map<String, Object> value) {
        checkDXLinkFormat(value);
        // TODO: how to set the environment?
        return DXDataObject.getInstance((String) value.get("$dnanexus_link"));
    }

    @VisibleForTesting
    static Map<String, AccessLevel> deserializeListProjectsMap(JsonNode result) {
        try {
            return listProjectsReader.<Map<String, AccessLevel>>readValue(result);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a {@code DXDataObject} corresponding to an existing object with the specified ID.
     *
     * @param objectId DNAnexus object id
     *
     * @return a {@code DXDataObject} handle to the specified object
     */
    public static DXDataObject getInstance(String objectId) {
        return getInstanceWithEnvironment(objectId, DXEnvironment.create());
    }

    /**
     * Returns a {@code DXDataObject} corresponding to an existing object with the specified ID in
     * the specified project or container.
     *
     * @param objectId DNAnexus object id
     * @param project project or container in which the object resides
     *
     * @return a {@code DXDataObject} handle to the specified object
     */
    public static DXDataObject getInstance(String objectId, DXContainer project) {
        return getInstanceWithEnvironment(objectId, project, DXEnvironment.create());
    }

    /**
     * Returns a {@code DXDataObject} corresponding to an existing object with the specified ID in
     * the specified project or container, using the specified environment, and with the specified
     * cached Describe data.
     *
     * @param objectId DNAnexus object id
     * @param project project or container in which the object resides
     * @param env environment to use to make subsequent API requests
     * @param describe cached Describe output
     *
     * @return a {@code DXDataObject} handle to the specified object
     */
    static DXDataObject getInstanceWithCachedDescribe(String objectId, DXContainer project,
            DXEnvironment env, JsonNode describe) {
        Preconditions.checkNotNull(describe);
        if (objectId.startsWith("record-")) {
            return DXRecord.getInstanceWithCachedDescribe(objectId, project, env, describe);
        } else if (objectId.startsWith("file-")) {
            return DXFile.getInstanceWithCachedDescribe(objectId, project, env, describe);
        } else if (objectId.startsWith("gtable-")) {
            return DXGTable.getInstanceWithCachedDescribe(objectId, project, env, describe);
        } else if (objectId.startsWith("applet-")) {
            return DXApplet.getInstanceWithCachedDescribe(objectId, project, env, describe);
        } else if (objectId.startsWith("workflow-")) {
            return DXWorkflow.getInstanceWithCachedDescribe(objectId, project, env, describe);
        }
        throw new IllegalArgumentException("The object ID " + objectId
                + " was of an unrecognized or unsupported class.");
    }

    /**
     * Returns a {@code DXDataObject} corresponding to an existing object with the specified ID in
     * the specified project or container, using the specified environment.
     *
     * @param objectId DNAnexus object id
     * @param project project or container in which the object resides
     * @param env environment to use to make subsequent API requests
     *
     * @return a {@code DXDataObject} handle to the specified object
     */
    public static DXDataObject getInstanceWithEnvironment(String objectId, DXContainer project,
            DXEnvironment env) {
        if (objectId.startsWith("record-")) {
            return DXRecord.getInstanceWithEnvironment(objectId, project, env);
        } else if (objectId.startsWith("file-")) {
            return DXFile.getInstanceWithEnvironment(objectId, project, env);
        } else if (objectId.startsWith("gtable-")) {
            return DXGTable.getInstanceWithEnvironment(objectId, project, env);
        } else if (objectId.startsWith("applet-")) {
            return DXApplet.getInstanceWithEnvironment(objectId, project, env);
        } else if (objectId.startsWith("workflow-")) {
            return DXWorkflow.getInstanceWithEnvironment(objectId, project, env);
        }
        throw new IllegalArgumentException("The object ID " + objectId
                + " was of an unrecognized or unsupported class.");
    }

    /**
     * Returns a {@code DXDataObject} corresponding to an existing object with the specified ID,
     * using the specified environment.
     *
     * @param objectId DNAnexus object id
     * @param env environment to use to make subsequent API requests
     *
     * @return a {@code DXDataObject} handle to the specified object
     */
    public static DXDataObject getInstanceWithEnvironment(String objectId, DXEnvironment env) {
        if (objectId.startsWith("record-")) {
            return DXRecord.getInstanceWithEnvironment(objectId, env);
        } else if (objectId.startsWith("file-")) {
            return DXFile.getInstanceWithEnvironment(objectId, env);
        } else if (objectId.startsWith("gtable-")) {
            return DXGTable.getInstanceWithEnvironment(objectId, env);
        } else if (objectId.startsWith("applet-")) {
            return DXApplet.getInstanceWithEnvironment(objectId, env);
        } else if (objectId.startsWith("workflow-")) {
            return DXWorkflow.getInstanceWithEnvironment(objectId, env);
        }
        throw new IllegalArgumentException("The object ID " + objectId
                + " was of an unrecognized or unsupported class.");
    }

    private final DXContainer container;
    // TODO: this might be useful to have in the superclass DXObject for other find* routes
    protected final JsonNode cachedDescribe;

    /**
     * Initializes the {@code DXDataObject} to point to the object with the specified ID in the
     * specified project.
     *
     * @param dxId DNAnexus ID of the data object
     * @param className class name that should prefix the ID
     * @param env environment to use for subsequent API requests from this {@code DXDataObject}, or
     *        null to use the default environment
     * @param cachedDescribe JSON hash of the describe output for this object if available, or null
     *        otherwise
     */
    protected DXDataObject(String dxId, String className, DXContainer project, DXEnvironment env,
            JsonNode cachedDescribe) {
        super(dxId, Preconditions.checkNotNull(className, "className may not be null"), env);
        this.container = Preconditions.checkNotNull(project, "project may not be null");
        // TODO: should we make a defensive copy?
        this.cachedDescribe = cachedDescribe;
    }

    /**
     * Initializes the {@code DXDataObject} to point to the object with the specified ID in the
     * environment's workspace.
     *
     * <p>
     * Operations that use or retrieve project-specific metadata will fail if the object does not
     * exist in the environment's workspace. When a project is available, you should prefer to set
     * it explicitly via {@link #DXDataObject(String, String, DXContainer, DXEnvironment, JsonNode)}
     * .
     * </p>
     *
     * @param dxId DNAnexus ID of the data object
     * @param className class name that should prefix the ID
     * @param env environment to use for subsequent API requests from this {@code DXDataObject}, or
     *        null to use the default environment
     * @param cachedDescribe JSON hash of the describe output for this object if available, or null
     *        otherwise
     */
    protected DXDataObject(String dxId, String className, DXEnvironment env, JsonNode cachedDescribe) {
        super(dxId, Preconditions.checkNotNull(className, "className may not be null"), env);
        this.container = null;
        // TODO: should we make a defensive copy?
        this.cachedDescribe = cachedDescribe;
    }

    /**
     * Adds the specified tags to the object.
     *
     * <p>
     * The tags are modified in the project or container associated with this {@code DXDataObject},
     * or the environment's workspace if no project or container was explicitly specified.
     * </p>
     *
     * @param tags List of tags to add to the object
     *
     * @throws NullPointerException if this object has no associated project and no workspace is set
     */
    public void addTags(List<String> tags) {
        apiCallOnObject("addTags",
                MAPPER.valueToTree(new AddOrRemoveTagsRequest(this.container.getId(), tags)),
                RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Adds the specified types to the object.
     *
     * @param types List of types to add to the object
     */
    public void addTypes(List<String> types) {
        apiCallOnObject("addTypes", MAPPER.valueToTree(new AddOrRemoveTypesRequest(types)),
                RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Verifies that this object carries cached describe data.
     *
     * @throws IllegalStateException if cachedDescribe is not set.
     */
    protected void checkCachedDescribeAvailable() throws IllegalStateException {
        if (this.cachedDescribe == null) {
            throw new IllegalStateException("This object contains no cached describe data.");
        }
    }

    /**
     * Closes the data object.
     *
     * <p>
     * Returns the same object so you can chain calls.
     * </p>
     *
     * @return the same {@code DXDataObject}
     */
    public DXDataObject close() {
        apiCallOnObject("close", RetryStrategy.SAFE_TO_RETRY);
        return this;
    }

    /**
     * Closes the data object and waits until the close operation is complete.
     *
     * <p>
     * Returns the same object so you can chain calls.
     * </p>
     *
     * @return the same {@code DXDataObject}
     */
    public DXDataObject closeAndWait() {
        DXDataObject obj = this.close();
        // TODO: allow supplying a timeout
        while (this.describe(DescribeOptions.get().withCustomFields(ImmutableList.of("state")))
                .getState() != DataObjectState.CLOSED) {
            // TODO: some kind of exponential backoff so short requests don't
            // take 2000ms to complete
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return obj;
    }

    /**
     * Returns metadata about the data object.
     *
     * <p>
     * The properties and details fields will not be returned, and any project-specific metadata
     * fields will be selected from an arbitrary project in which the requesting user has access to
     * this object. To change either of these aspects of this behavior, use
     * {@link #describe(DescribeOptions)} instead.
     * </p>
     *
     * @return a {@code Describe} containing the data object's metadata.
     */
    public Describe describe() {
        return DXJSON.safeTreeToValue(apiCallOnObject("describe", RetryStrategy.SAFE_TO_RETRY),
                Describe.class);
    }

    /**
     * Returns metadata about the data object, specifying which optional fields are to be returned
     * and what project to obtain project-specific metadata from.
     *
     * @param options {@code DescribeOptions} object specifying how the {@code describe} request is
     *        to be made.
     *
     * @return a {@code Describe} containing the data object's metadata.
     */
    public Describe describe(DescribeOptions options) {
        return DXJSON.safeTreeToValue(
                apiCallOnObject("describe", MAPPER.valueToTree(options),
                        RetryStrategy.SAFE_TO_RETRY), Describe.class);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof DXDataObject)) {
            return false;
        }
        DXDataObject other = (DXDataObject) obj;
        if (container == null) {
            if (other.container != null) {
                return false;
            }
        } else if (!container.equals(other.container)) {
            return false;
        }
        return true;
    }

    /**
     * Returns metadata about the data object, like {@link DXDataObject#describe()}, but without
     * making an API call.
     *
     * <p>
     * This cached describe info is only available if this object appears in the result of a
     * {@link DXSearch#findDataObjects()} call that specified
     * {@link DXSearch.FindDataObjectsRequestBuilder#includeDescribeOutput()}, and the describe info
     * that is returned reflects the state of the object at the time that the search was performed.
     * </p>
     *
     * @return a {@code Describe} containing the data object's metadata
     *
     * @throws IllegalStateException if no cached describe info is available
     */
    public Describe getCachedDescribe() {
        this.checkCachedDescribeAvailable();
        return DXJSON.safeTreeToValue(this.cachedDescribe, Describe.class);
    }

    /**
     * Returns a DNAnexus link for this object. This is the JSON serializer so it makes it suitable
     * to provide an object with references to DXDataObjects as the input when running a
     * {@link DXApplet}, etc.
     *
     * @return a DNAnexus link
     */
    @SuppressWarnings("unused")
    @JsonValue
    private JsonNode getDXLink() {
        return DXJSON.getObjectBuilder().put("$dnanexus_link", this.getId()).build();
    }

    /**
     * Returns the object's project or container, if it was explicitly supplied.
     *
     * @return project or container, or {@code null} if none was specified at object creation time
     */
    public DXContainer getProject() {
        return this.container;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((container == null) ? 0 : container.hashCode());
        return result;
    }

    /**
     * Returns the set of projects that contain this object, and which the requesting user has
     * permissions to access.
     *
     * @return Mapping from project ID to the user's access level in that project.
     */
    public Map<DXContainer, AccessLevel> listProjects() {
        Map<String, AccessLevel> rawMap =
                deserializeListProjectsMap(apiCallOnObject("listProjects",
                        RetryStrategy.SAFE_TO_RETRY));
        ImmutableMap.Builder<DXContainer, AccessLevel> resultBuilder = ImmutableMap.builder();
        for (Map.Entry<String, AccessLevel> entry : rawMap.entrySet()) {
            resultBuilder.put(DXContainer.getInstance(entry.getKey()), entry.getValue());
        }
        return resultBuilder.build();
    }

    /**
     * Sets properties on the object.
     *
     * <p>
     * The properties are modified in the project or container associated with this
     * {@code DXDataObject}, or the environment's workspace if no project or container was
     * explicitly specified.
     * </p>
     *
     * @param properties Map from key to value for each property to be set
     *
     * @throws NullPointerException if this object has no associated project and no workspace is set
     */
    public void putAllProperties(Map<String, String> properties) {
        putAllProperties(properties, ImmutableList.<String>of());
    }

    /**
     * Sets and removes properties on the object.
     *
     * <p>
     * The properties are modified in the project or container associated with this
     * {@code DXDataObject}, or the environment's workspace if no project or container was
     * explicitly specified.
     * </p>
     *
     * @param propertiesToSet Map from key to value for each property to be set
     * @param propertiesToRemove List of property keys to be removed
     *
     * @throws NullPointerException if this object has no associated project and no workspace is set
     */
    public void putAllProperties(Map<String, String> propertiesToSet,
            List<String> propertiesToRemove) {
        Preconditions.checkNotNull(this.container,
                "Container must be supplied for this metadata operation");
        apiCallOnObject("setProperties", MAPPER.valueToTree(new SetPropertiesRequest(this.container
                .getId(), propertiesToSet, propertiesToRemove)), RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Sets a property on the object.
     *
     * <p>
     * The properties are modified in the project or container associated with this
     * {@code DXDataObject}, or the environment's workspace if no project or container was
     * explicitly specified.
     * </p>
     *
     * @param key property key to set
     * @param value property value to set
     *
     * @throws NullPointerException if this object has no associated project and no workspace is set
     */
    public void putProperty(String key, String value) {
        putAllProperties(ImmutableMap.of(key, value));
    }

    /**
     * Removes a property from the object.
     *
     * <p>
     * The properties are modified in the project or container associated with this
     * {@code DXDataObject}, or the environment's workspace if no project or container was
     * explicitly specified.
     * </p>
     *
     * @param key property key to be removed
     *
     * @throws NullPointerException if this object has no associated project and no workspace is set
     */
    public void removeProperty(String key) {
        putAllProperties(ImmutableMap.<String, String>of(), ImmutableList.of(key));
    }

    /**
     * Removes the specified tags from the object.
     *
     * <p>
     * The tags are modified in the project or container associated with this {@code DXDataObject},
     * or the environment's workspace if no project or container was explicitly specified.
     * </p>
     *
     * @param tags List of tags to remove
     *
     * @throws NullPointerException if this object has no associated project and no workspace is set
     */
    public void removeTags(List<String> tags) {
        Preconditions.checkNotNull(this.container,
                "Container must be supplied for this metadata operation");
        apiCallOnObject("removeTags",
                MAPPER.valueToTree(new AddOrRemoveTagsRequest(this.container.getId(), tags)),
                RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Removes the specified types from the object.
     *
     * @param types List of types to remove
     */
    public void removeTypes(List<String> types) {
        apiCallOnObject("removeTypes", MAPPER.valueToTree(new AddOrRemoveTypesRequest(types)),
                RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Changes the name of the object in its project. The basename of the object is changed to the
     * specified name and it remains in the same folder.
     *
     * <p>
     * The object is renamed in the project or container associated with this {@code DXDataObject},
     * or the environment's workspace if no project or container was explicitly specified.
     * </p>
     *
     * @param newName The new name of the object
     *
     * @throws NullPointerException if this object has no associated project and no workspace is set
     */
    public void rename(String newName) {
        Preconditions.checkNotNull(this.container,
                "Container must be supplied for this metadata operation");
        apiCallOnObject("removeTags",
                MAPPER.valueToTree(new RenameRequest(this.container.getId(), newName)),
                RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Sets the details of the object.
     *
     * @param details an object whose JSON serialized form will be set as the details
     */
    public void setDetails(Object details) {
        apiCallOnObject("setDetails", MAPPER.valueToTree(details), RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Makes the object visible or hidden.
     *
     * @param visible
     */
    public void setVisibility(boolean visible) {
        apiCallOnObject("setVisibility", MAPPER.valueToTree(new SetVisibilityRequest(!visible)),
                RetryStrategy.SAFE_TO_RETRY);
    }

}
