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

package com.dnanexus.exceptions;

/**
 * The input is syntactically correct but semantically incorrect.
 * 
 * <p>
 * For example, a JSON array is provided where a hash was required, or a
 * required parameter is missing, etc.
 * </p>
 */
public class InvalidInputException extends DXAPIException {

    /**
     * Creates an {@code InvalidInputException} with the specified message and
     * HTTP status code.
     */
    public InvalidInputException(String message, int statusCode) {
        super(message, statusCode);
    }

    @Override
    public String toString() {
        return "InvalidInput: " + this.getMessage();
    }

    private static final long serialVersionUID = -5147921369506132093L;

}
