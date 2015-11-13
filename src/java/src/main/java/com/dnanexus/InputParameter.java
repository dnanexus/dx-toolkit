// Copyright (C) 2013-2015 DNAnexus, Inc.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * An input parameter for an executable.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class InputParameter {

    /**
     * Builder class for creating new input parameters. To obtain an instance, call
     * {@link InputParameter#newInputParameter(String, IOClass)}.
     */
    public static class Builder {
        private String name;
        private IOClass ioClass;
        private boolean optional;

        private Builder(String name, IOClass parameterClass) {
            this.name = name;
            this.ioClass = parameterClass;
        }

        /**
         * Builds the input parameter object.
         *
         * @return the resulting parameter
         */
        public InputParameter build() {
            return new InputParameter(this);
        }

        /**
         * Makes this input parameter optional.
         *
         * @return the same builder object
         */
        public Builder optional() {
            this.optional = true;
            return this;
        }
    }

    /**
     * Returns a new input parameter builder initialized with the specified input name and class.
     *
     * @param name name of the input to be created
     * @param parameterClass class of the input to be created
     *
     * @return an input parameter builder
     */
    public static Builder newInputParameter(String name, IOClass parameterClass) {
        return new Builder(Preconditions.checkNotNull(name, "name may not be null"),
                Preconditions.checkNotNull(parameterClass, "parameterClass may not be null"));
    }

    @JsonProperty
    private String name;

    // TODO: default, type, patterns, suggestions, choices, group, help

    @JsonProperty("class")
    private IOClass ioClass;
    @JsonProperty
    private String label;  // TODO: needs getters/setters
    @JsonProperty
    private Boolean optional;

    private InputParameter() {
        // No-arg constructor for Jackson deserialization
    }

    private InputParameter(Builder builder) {
        this.name = builder.name;
        this.ioClass = builder.ioClass;
        if (builder.optional) {
            this.optional = builder.optional;
        } else {
            this.optional = null;
        }
    }

    /**
     * Returns the class of the parameter.
     *
     * @return class name
     */
    public IOClass getIOClass() {
        return ioClass;
    }

    /**
     * Returns the name of the parameter.
     *
     * @return parameter name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns whether the parameter is optional.
     *
     * @return true if optional
     */
    public boolean isOptional() {
        if (optional == null) {
            return false;
        }
        return optional;
    }


}
