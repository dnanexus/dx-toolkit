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

import java.util.List;
import java.util.Map;

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * An applet (an executable data object).
 *
 * <p>
 * Although these bindings will allow you to create a simple applet from scratch, we encourage you
 * to use the command-line tool <code>dx build</code> instead. See the <a
 * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets-and-Entry-Points">API
 * documentation for applets</a>.
 * </p>
 *
 * <p>
 * The {@link #describe()} method returns a {@link RunSpecification} object that does not have the
 * "code" field populated; that is, {@link RunSpecification#getCode()} will return {@code null}.
 * </p>
 */
public class DXApplet extends DXDataObject implements DXExecutable<DXJob> {

    @JsonInclude(Include.NON_NULL)
    private static class AppletNewRequest extends DataObjectNewRequest {
        @SuppressWarnings("unused")
        @JsonProperty
        private final String title;
        @SuppressWarnings("unused")
        @JsonProperty
        private final String summary;
        @SuppressWarnings("unused")
        @JsonProperty
        private final String description;
        @SuppressWarnings("unused")
        @JsonProperty
        private final List<InputParameter> inputSpec;
        @SuppressWarnings("unused")
        @JsonProperty
        private final List<OutputParameter> outputSpec;
        @SuppressWarnings("unused")
        @JsonProperty
        private final RunSpecification runSpec;
        @SuppressWarnings("unused")
        @JsonProperty
        private final String dxapi;

        public AppletNewRequest(Builder builder) {
            super(builder);

            this.title = builder.title;
            this.summary = builder.summary;
            this.description = builder.description;
            this.inputSpec = builder.inputSpec;
            this.outputSpec = builder.outputSpec;
            this.runSpec = builder.runSpec;
            this.dxapi = builder.dxapi;
        }
    }

    /**
     * Builder class for creating a new {@code DXApplet} object. To obtain an instance, call
     * {@link DXApplet#newApplet()}.
     */
    public static class Builder extends DXDataObject.Builder<Builder, DXApplet> {
        private String title;
        private String summary;
        private String description;
        private List<InputParameter> inputSpec;
        private List<OutputParameter> outputSpec;
        private RunSpecification runSpec;
        private String dxapi = "1.0.0";

        // TODO: access hash

        private Builder() {
            super();
        }

        private Builder(DXEnvironment env) {
            super(env);
        }

        /**
         * Creates the applet.
         *
         * @return a {@code DXApplet} object corresponding to the newly created object
         */
        @Override
        public DXApplet build() {
            return new DXApplet(DXAPI.appletNew(this.buildRequestHash(), ObjectNewResponse.class,
                    this.env).getId(), this.project, this.env, null);
        }

        /**
         * Use this method to test the JSON hash created by a particular builder call without
         * actually executing the request.
         *
         * @return a JsonNode
         */
        @VisibleForTesting
        JsonNode buildRequestHash() {
            checkAndFixParameters();
            return MAPPER.valueToTree(new AppletNewRequest(this));
        }

        /**
         * Ensures that the project was either explicitly set or that the environment specifies a
         * workspace, and that the run specification is provided.
         */
        @Override
        protected void checkAndFixParameters() {
            super.checkAndFixParameters();
            Preconditions.checkState(this.runSpec != null, "setRunSpecification must be specified");
        }

        /*
         * (non-Javadoc)
         *
         * @see com.dnanexus.DXDataObject.Builder#getThisInstance()
         */
        @Override
        protected Builder getThisInstance() {
            return this;
        }

        /**
         * Sets the description of the applet to be created.
         *
         * @param description applet description
         *
         * @return the same {@code Builder} object
         */
        public Builder setDescription(String description) {
            Preconditions.checkState(this.description == null,
                    "Cannot call setDescription more than once");
            this.description =
                    Preconditions.checkNotNull(description, "description may not be null");
            return getThisInstance();
        }

        /**
         * Sets the input specification of the applet to be created.
         *
         * @param inputSpec input specification
         *
         * @return the same {@code Builder} object
         */
        public Builder setInputSpecification(List<? extends InputParameter> inputSpec) {
            Preconditions.checkState(this.inputSpec == null,
                    "Cannot call setInputSpecification more than once");
            this.inputSpec =
                    ImmutableList.copyOf(Preconditions.checkNotNull(inputSpec,
                            "inputSpec may not be null"));
            return getThisInstance();
        }

        /**
         * Sets the output specification of the applet to be created.
         *
         * @param outputSpec output specification
         *
         * @return the same {@code Builder} object
         */
        public Builder setOutputSpecification(List<? extends OutputParameter> outputSpec) {
            Preconditions.checkState(this.outputSpec == null,
                    "Cannot call setOutputSpecification more than once");
            this.outputSpec =
                    ImmutableList.copyOf(Preconditions.checkNotNull(outputSpec,
                            "outputSpec may not be null"));
            return getThisInstance();
        }

        /**
         * Sets the run specification of the applet to be created.
         *
         * @param runSpec applet run specification
         *
         * @return the same {@code Builder} object
         */
        public Builder setRunSpecification(RunSpecification runSpec) {
            Preconditions.checkState(this.runSpec == null,
                    "Cannot call setRunSpecification more than once");
            this.runSpec = Preconditions.checkNotNull(runSpec, "runSpec may not be null");
            return getThisInstance();
        }

        /**
         * Sets the summary of the applet to be created.
         *
         * @param summary applet summary
         *
         * @return the same {@code Builder} object
         */
        public Builder setSummary(String summary) {
            Preconditions.checkState(this.summary == null, "Cannot call setSummary more than once");
            this.summary = Preconditions.checkNotNull(summary, "summary may not be null");
            return getThisInstance();
        }

        /**
         * Sets the title of the applet to be created.
         *
         * @param title applet title
         *
         * @return the same {@code Builder} object
         */
        public Builder setTitle(String title) {
            Preconditions.checkState(this.title == null, "Cannot call setTitle more than once");
            this.title = Preconditions.checkNotNull(title, "title may not be null");
            return getThisInstance();
        }

    }

    /**
     * Contains metadata for an applet.
     */
    public static class Describe extends DXDataObject.Describe {
        @JsonProperty
        private String title;
        @JsonProperty
        private String summary;
        @JsonProperty
        private String description;
        @JsonProperty
        private List<InputParameter> inputSpec;
        @JsonProperty
        private List<OutputParameter> outputSpec;
        @JsonProperty
        private RunSpecification runSpec;
        @JsonProperty
        private String dxapi;

        private Describe() {
            super();
        }

        /**
         * Returns the applet description.
         *
         * @return applet description
         */
        public String getDescription() {
            Preconditions
                    .checkState(this.description != null,
                            "description is not available because it was not retrieved with the describe call");
            return description;
        }

        /**
         * Returns the API version that the applet code is to run under.
         *
         * @return API version
         */
        public String getDXAPIVersion() {
            Preconditions
                    .checkState(this.dxapi != null,
                            "dxapi version is not available because it was not retrieved with the describe call");
            return dxapi;
        }

        /**
         * Returns the applet's input specification.
         *
         * @return applet input specification
         */
        public List<InputParameter> getInputSpecification() {
            Preconditions
                    .checkState(this.inputSpec != null,
                            "input specification is not available because it was not retrieved with the describe call");
            return ImmutableList.copyOf(this.inputSpec);
        }

        /**
         * Returns the applet's output specification.
         *
         * @return applet output specification
         */
        public List<OutputParameter> getOutputSpecification() {
            Preconditions
                    .checkState(this.outputSpec != null,
                            "output specification is not available because it was not retrieved with the describe call");
            return ImmutableList.copyOf(this.outputSpec);
        }

        /**
         * Returns the applet's run specification.
         *
         * @return applet run specification
         */
        public RunSpecification getRunSpecification() {
            Preconditions
                    .checkState(this.runSpec != null,
                            "run specification is not available because it was not retrieved with the describe call");
            return runSpec;
        }

        /**
         * Returns the applet summary.
         *
         * @return applet summary
         */
        public String getSummary() {
            Preconditions.checkState(this.summary != null,
                    "summary is not available because it was not retrieved with the describe call");
            return summary;
        }

        /**
         * Returns the applet title.
         *
         * @return applet title
         */
        public String getTitle() {
            Preconditions.checkState(this.title != null,
                    "title is not available because it was not retrieved with the describe call");
            return title;
        }
    }

    /**
     * Deserializes a DXApplet from JSON containing a DNAnexus link.
     *
     * @param value JSON object map
     *
     * @return data object
     */
    @SuppressWarnings("unused")
    @JsonCreator
    private static DXApplet create(Map<String, Object> value) {
        checkDXLinkFormat(value);
        // TODO: how to set the environment?
        return DXApplet.getInstance((String) value.get("$dnanexus_link"));
    }

    /**
     * Returns a {@code DXApplet} associated with an existing applet.
     *
     * @throws NullPointerException If {@code appletId} is null
     */
    public static DXApplet getInstance(String appletId) {
        return new DXApplet(appletId, null);
    }

    /**
     * Returns a {@code DXApplet} associated with an existing applet in a particular project or
     * container.
     *
     * @throws NullPointerException If {@code appletId} or {@code container} is null
     */
    public static DXApplet getInstance(String appletId, DXContainer project) {
        return new DXApplet(appletId, project, null, null);
    }

    /**
     * Returns a {@code DXApplet} associated with an existing applet in a particular project using
     * the specified environment, with the specified cached describe output.
     *
     * <p>
     * This method is for use exclusively by bindings to the "find" routes when describe hashes are
     * returned with the find output.
     * </p>
     *
     * @throws NullPointerException If any argument is null
     */
    static DXApplet getInstanceWithCachedDescribe(String appletId, DXContainer project,
            DXEnvironment env, JsonNode describe) {
        return new DXApplet(appletId, project, Preconditions.checkNotNull(env,
                "env may not be null"), Preconditions.checkNotNull(describe,
                "describe may not be null"));
    }

    /**
     * Returns a {@code DXApplet} associated with an existing applet in a particular project using
     * the specified environment.
     *
     * @throws NullPointerException If {@code appletId} or {@code container} is null
     */
    public static DXApplet getInstanceWithEnvironment(String appletId, DXContainer project,
            DXEnvironment env) {
        return new DXApplet(appletId, project, Preconditions.checkNotNull(env,
                "env may not be null"), null);
    }

    /**
     * Returns a {@code DXApplet} associated with an existing applet using the specified
     * environment.
     *
     * @throws NullPointerException If {@code appletId} is null
     */
    public static DXApplet getInstanceWithEnvironment(String appletId, DXEnvironment env) {
        return new DXApplet(appletId, Preconditions.checkNotNull(env, "env may not be null"));
    }

    /**
     * Returns a Builder object for creating a new {@code DXApplet}.
     *
     * @return Builder object
     */
    public static Builder newApplet() {
        return new Builder();
    }

    /**
     * Returns a Builder object for creating a new {@code DXApplet} using the specified environment.
     *
     * @param env environment to be used for subsequent API calls
     *
     * @return Builder object
     */
    public static Builder newAppletWithEnvironment(DXEnvironment env) {
        return new Builder(env);
    }

    private DXApplet(String appletId, DXContainer project, DXEnvironment env, JsonNode describe) {
        super(appletId, "applet", project, env, describe);
    }

    private DXApplet(String appletId, DXEnvironment env) {
        super(appletId, "applet", env, null);
    }

    @Override
    public Describe describe() {
        // TODO: add a flag to get the full runSpec (this should call applet-xxxx/get instead of
        // applet-xxxx/describe).
        return DXJSON.safeTreeToValue(apiCallOnObject("describe", RetryStrategy.SAFE_TO_RETRY),
                Describe.class);
    }

    @Override
    public Describe describe(DescribeOptions options) {
        return DXJSON.safeTreeToValue(
                apiCallOnObject("describe", MAPPER.valueToTree(options),
                        RetryStrategy.SAFE_TO_RETRY), Describe.class);
    }

    @Override
    public Describe getCachedDescribe() {
        this.checkCachedDescribeAvailable();
        return DXJSON.safeTreeToValue(this.cachedDescribe, Describe.class);
    }

    @Override
    public ExecutableRunner<DXJob> newRun() {
        return ExecutableRunner.getAppletRunnerWithEnvironment(this.getId(), this.env);
    }

}
