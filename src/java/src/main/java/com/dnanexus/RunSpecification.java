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

        private Builder(String interpreter, String code) {
            this.interpreter = interpreter;
            this.code = code;
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
     * Returns a builder initialized to create a run specification with the given interpreter and
     * entry point code.
     *
     * @param interpreter interpreter name, e.g. "bash" or "python2.7"
     * @param code entry point code
     *
     * @return Builder object
     */
    public static Builder newRunSpec(String interpreter, String code) {
        return new Builder(interpreter, code);
    }

    @JsonProperty
    private String code;

    @JsonProperty
    private String interpreter;

    private RunSpecification() {
        // No-arg constructor for Jackson deserialization
    }

    private RunSpecification(Builder builder) {
        this.code = builder.code;
        this.interpreter = builder.interpreter;
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

}
