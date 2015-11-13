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
 * An output parameter for an executable.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class OutputParameter {

    /**
     * Builder class for creating new output parameters. To obtain an instance, call
     * {@link OutputParameter#newOutputParameter(String, IOClass)}.
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
         * Builds the output parameter object.
         *
         * @return the resulting parameter
         */
        public OutputParameter build() {
            return new OutputParameter(this);
        }

        /**
         * Makes this output parameter optional.
         *
         * @return the same builder object
         */
        public Builder optional() {
            this.optional = true;
            return this;
        }
    }

    /**
     * Returns a new output parameter builder initialized with the specified output name and class.
     *
     * @param name name of the output to be created
     * @param parameterClass class of the output to be created
     *
     * @return an output parameter builder
     */
    public static Builder newOutputParameter(String name, IOClass parameterClass) {
        return new Builder(Preconditions.checkNotNull(name, "name may not be null"),
                Preconditions.checkNotNull(parameterClass, "parameterClass may not be null"));
    }

    @JsonProperty
    private String name;

    // TODO: type, patterns, group, help

    @JsonProperty("class")
    private IOClass ioClass;
    @JsonProperty
    private String label;  // TODO: needs getters/setters
    @JsonProperty
    private Boolean optional;

    private OutputParameter() {
        // No-arg constructor for Jackson deserialization
    }

    private OutputParameter(Builder builder) {
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
