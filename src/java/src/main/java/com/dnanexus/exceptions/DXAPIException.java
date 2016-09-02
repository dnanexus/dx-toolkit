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

package com.dnanexus.exceptions;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Represents any response from the API server other than HTTP code 200 (OK).
 */
public class DXAPIException extends RuntimeException {

    private static enum ApiExceptionClass {
        InternalError("InternalError") {
          @Override
          DXAPIException generateException(String message, int statusCode) {
              return new InternalErrorException(message, statusCode);
          }
        },
        InvalidAuthentication("InvalidAuthentication") {
            @Override
            DXAPIException generateException(String message, int statusCode) {
                return new InvalidAuthenticationException(message, statusCode);
            }
        },
        InvalidInput("InvalidInput") {
            @Override
            DXAPIException generateException(String message, int statusCode) {
                return new InvalidInputException(message, statusCode);
            }
        },
        InvalidState("InvalidState") {
            @Override
            DXAPIException generateException(String message, int statusCode) {
                return new InvalidStateException(message, statusCode);
            }
        },
        InvalidType("InvalidType") {
            @Override
            DXAPIException generateException(String message, int statusCode) {
                return new InvalidTypeException(message, statusCode);
            }
        },
        PermissionDenied("PermissionDenied") {
            @Override
            DXAPIException generateException(String message, int statusCode) {
                return new PermissionDeniedException(message, statusCode);
            }
        },
        ResourceNotFound("ResourceNotFound") {
            @Override
            DXAPIException generateException(String message, int statusCode) {
                return new ResourceNotFoundException(message, statusCode);
            }
        },
        ServiceUnavailableException("ServiceUnavailable") {
            @Override
            DXAPIException generateException(String message, int statusCode) {
                return new ServiceUnavailableException(message, statusCode);
            }
        },
        SpendingLimitExceeded("SpendingLimitExceeded") {
            @Override
            DXAPIException generateException(String message, int statusCode) {
                return new SpendingLimitExceededException(message, statusCode);
            }
        };

        private String errorType;

        private ApiExceptionClass(String errorType) {
            this.errorType = errorType;
        }

        abstract DXAPIException generateException(String message, int statusCode);

    }

    private static Map<String, ApiExceptionClass> valueMap;
    static {
        valueMap = Maps.newHashMap();
        for (ApiExceptionClass c : ApiExceptionClass.values()) {
            valueMap.put(c.errorType, c);
        }
        valueMap = ImmutableMap.copyOf(valueMap);
    }

    /**
     * Constructs a {@code DXAPIException} of the appropriate subclass based on
     * the {@code errorType}. If the {@code errorType} is not one of the
     * recognized subclasses, a generic {@code DXAPIException} object is
     * returned.
     *
     * @param errorType
     *            String containing a DNAnexus error type, e.g.
     *            {@code InvalidInput}
     * @param errorMessage
     *            String indicating the nature of the error.
     * @param statusCode
     *            HTTP status code of the response body.
     * @return Instance of {@code DXAPIException} or one of its subclasses.
     */
    public static DXAPIException getInstance(String errorType, String errorMessage, int statusCode) {
        String errorMessageOrDefault = errorMessage;
        if (errorMessageOrDefault == null) {
            errorMessageOrDefault = "(Unparseable error message)";
        }
        if (errorType != null && valueMap.containsKey(errorType)) {
            return valueMap.get(errorType).generateException(errorMessageOrDefault, statusCode);
        }
        System.err.println("[" + System.currentTimeMillis() + "] " + "Received an API error of unknown type " + errorType
                + "; deserializing it as a generic DXAPIException.");
        return new DXAPIException(errorMessageOrDefault, statusCode);
    }

    /**
     * HTTP status code associated with the error.
     */
    private final int statusCode;

    /**
     * Initializes a new exception with the specified message and HTTP status
     * code.
     */
    public DXAPIException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    /**
     * Returns the HTTP status code associated with the error.
     */
    public int getStatusCode() {
        return this.statusCode;
    }

    @Override
    public String toString() {
        return "DXAPIException: " + this.getMessage();
    }

    private static final long serialVersionUID = 1479149805498384423L;

}
