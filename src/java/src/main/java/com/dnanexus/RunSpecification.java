// Copyright (C) 2013-2016 DNAnexus, Inc.
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

/**
 * A run specification for an applet.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class RunSpecification {

    /**
     * Builder class for creating RunSpecifications.
     */
    public static class Builder {

        private String interpreter;
        private String code;
        private String distribution;
        private String release;

        private Builder(String interpreter, String code, String distribution, String release) {
            this.interpreter = interpreter;
            this.code = code;
            this. distribution = distribution;
            this.release = release;
        }

        /**
         * Creates the run specification.
         *
         * @return run specification
         */
        public RunSpecification build() {
            return new RunSpecification(this);
        }

        // TODO: systemRequirements, executionPolicy, bundledDepends, execDepends
    }

    /**
     * Returns a builder initialized to create a run specification with the given interpreter,
     * entry point code, distribution and releae.
     *
     * @param interpreter interpreter name, e.g. "bash" or "python2.7"
     * @param code entry point code
     * @param distribution OS distribution, e.g. "Ubuntu"
     * @param release OS release, e.g. "14.04"
     *
     * @return Builder object
     */
    public static Builder newRunSpec(String interpreter, String code, String distribution, String release) {
        return new Builder(interpreter, code, distribution, release);
    }

    @JsonProperty
    private String code;

    @JsonProperty
    private String interpreter;

    @JsonProperty
    private String distribution;

    @JsonProperty
    private String release;

    private RunSpecification() {
        // No-arg constructor for Jackson deserialization
    }

    private RunSpecification(Builder builder) {
        this.code = builder.code;
        this.interpreter = builder.interpreter;
        this.distribution = builder.distribution;
        this.release = builder.release;
    }

    /**
     * Returns the entry point code for the applet.
     *
     * @return entry point code (in whatever language is specified by the interpreter)
     */
    public String getCode() {
        return code;
    }

    /**
     * Returns the interpreter for the applet.
     *
     * @return the interpreter
     */
    public String getInterpreter() {
        return interpreter;
    }

    /**
     * Returns the distribution for the applet.
     *
     * @return the distribution
     */
    public String getDistribution() {
        return distribution;
    }

    /**
     * Returns the release for the applet.
     *
     * @return the release
     */
    public String getRelease() {
        return release;
    }
}
