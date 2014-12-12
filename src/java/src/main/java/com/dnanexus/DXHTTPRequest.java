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
import java.nio.charset.Charset;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.util.EntityUtils;

import com.dnanexus.exceptions.DXAPIException;
import com.dnanexus.exceptions.DXHTTPException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Class for making a raw DNAnexus API call via HTTP.
 */
public class DXHTTPRequest {
    /**
     * Holds either the raw text of a response or a parsed JSON version of it.
     */
    private static class ParsedResponse {
        public final String responseText;
        public final JsonNode responseJson;

        public ParsedResponse(String responseText, JsonNode responseJson) {
            this.responseText = responseText;
            this.responseJson = responseJson;
        }
    }

    /**
     * Indicates whether a particular API request can be retried.
     *
     * <p>
     * See the <a
     * href="https://github.com/dnanexus/dx-toolkit/blob/master/src/api_wrappers/README.md">API
     * wrappers common documentation</a> for the retry logic specification.
     * </p>
     */
    public static enum RetryStrategy {
        /**
         * The request has non-idempotent side effects and is generally not safe to retry if the
         * outcome of a previous request is unknown.
         */
        UNSAFE_TO_RETRY,
        /**
         * The request is idempotent and is safe to retry.
         */
        SAFE_TO_RETRY;
    }

    /**
     * Internal exception used to indicate that the request yielded 503 Service Unavailable and
     * suggested that we retry at some point in the future.
     */
    @SuppressWarnings("serial")
    private static class ServiceUnavailableException extends Exception {
        private final int secondsToWaitForRetry;

        public ServiceUnavailableException(int secondsToWaitForRetry) {
            this.secondsToWaitForRetry = secondsToWaitForRetry;
        }
    }

    /**
     * Sleeps for the specified amount of time. Throws a {@link RuntimeException} if interrupted.
     *
     * @param seconds number of seconds to sleep for
     */
    private static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private final JsonNode securityContext;

    private final String apiserver;

    private final DefaultHttpClient httpclient;

    private static final int NUM_RETRIES = 6;

    private static final DXEnvironment defaultEnv = DXEnvironment.create();

    private static final String USER_AGENT = DXUserAgent.getUserAgent();

    private static String errorMessage(String method, String resource, String errorString,
            int retryWait, int nextRetryNum, int maxRetries) {
        String baseError = method + " " + resource + ": " + errorString + ".";
        if (nextRetryNum <= maxRetries) {
            return baseError + "  Waiting " + retryWait + " seconds before retry " + nextRetryNum
                    + " of " + maxRetries;
        }
        return baseError;
    }

    /**
     * Construct the DXHTTPRequest using the default DXEnvironment.
     */
    public DXHTTPRequest() {
        this(defaultEnv);
    }

    /**
     * Construct the DXHTTPRequest using the given DXEnvironment.
     */
    public DXHTTPRequest(DXEnvironment env) {
        this.securityContext = env.getSecurityContextJson();
        this.apiserver = env.getApiserverPath();
        this.httpclient = new DefaultHttpClient();
        httpclient.getParams().setParameter(CoreProtocolPNames.USER_AGENT, USER_AGENT);
    }

    /**
     * Issues a request against the specified resource (assuming requests ARE safe to be retried)
     * and returns the result as a JSON object.
     *
     * @param resource Name of resource, e.g. "/file-XXXX/describe"
     * @param data Request payload (to be converted to JSON)
     *
     * @deprecated Use {@link #request(String, JsonNode, RetryStrategy)} instead.
     *
     * @throws DXAPIException If the server returns a complete response with an HTTP status code
     *         other than 200 (OK).
     * @throws DXHTTPException If an error occurs while making the HTTP request or obtaining the
     *         response (includes HTTP protocol errors).
     */
    @Deprecated
    public JsonNode request(String resource, JsonNode data) {
        return request(resource, data, RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Issues a request against the specified resource and returns the result as a JSON object.
     *
     * @param resource Name of resource, e.g. "/file-XXXX/describe"
     * @param data Request payload (to be converted to JSON)
     * @param retryStrategy Indicates whether the request is idempotent and can be retried
     *
     * @throws DXAPIException If the server returns a complete response with an HTTP status code
     *         other than 200 (OK).
     * @throws DXHTTPException If an error occurs while making the HTTP request or obtaining the
     *         response (includes HTTP protocol errors).
     */
    public JsonNode request(String resource, JsonNode data, RetryStrategy retryStrategy) {
        String dataAsString = data.toString();
        return requestImpl(resource, dataAsString, true, retryStrategy).responseJson;
    }

    /**
     * Issues a request against the specified resource (assuming requests ARE safe to be retried)
     * and returns the result as a String.
     *
     * @param resource Name of resource, e.g. "/file-XXXX/describe"
     * @param data Request payload (String to be sent verbatim)
     *
     * @deprecated Use {@link #request(String, String, RetryStrategy)} instead.
     *
     * @throws DXAPIException If the server returns a complete response with an HTTP status code
     *         other than 200 (OK).
     * @throws DXHTTPException If an error occurs while making the HTTP request or obtaining the
     *         response (includes HTTP protocol errors).
     */
    @Deprecated
    public String request(String resource, String data) {
        return request(resource, data, RetryStrategy.SAFE_TO_RETRY);
    }

    /**
     * Issues a request against the specified resource and returns the result as a String.
     *
     * @param resource Name of resource, e.g. "/file-XXXX/describe"
     * @param data Request payload (String to be sent verbatim)
     * @param retryStrategy Indicates whether the request is idempotent and can be retried
     *
     * @throws DXAPIException If the server returns a complete response with an HTTP status code
     *         other than 200 (OK).
     * @throws DXHTTPException If an error occurs while making the HTTP request or obtaining the
     *         response (includes HTTP protocol errors).
     */
    public String request(String resource, String data, RetryStrategy retryStrategy) {
        return requestImpl(resource, data, false, retryStrategy).responseText;
    }

    /**
     * Issues a request against the specified resource and returns either the text of the response
     * or the parsed JSON of the response (depending on whether parseResponse is set).
     *
     * @throws DXAPIException If the server returns a complete response with an HTTP status code
     *         other than 200 (OK).
     * @throws DXHTTPException If an error occurs while making the HTTP request or obtaining the
     *         response (includes HTTP protocol errors).
     */
    private ParsedResponse requestImpl(String resource, String data, boolean parseResponse,
            RetryStrategy retryStrategy) {
        HttpPost request = new HttpPost(apiserver + resource);

        if (securityContext == null) {
            throw new DXHTTPException(new IOException("No security context was set"));
        }

        request.setHeader("Content-Type", "application/json");
        request.setHeader("Authorization", securityContext.get("auth_token_type").textValue() + " "
                + securityContext.get("auth_token").textValue());
        request.setEntity(new StringEntity(data, Charset.forName("UTF-8")));

        // Retry with exponential backoff
        int timeoutSeconds = 1;
        int attempts = 0;

        while (true) {
            // This guarantees that we get at least one iteration around this loop before running
            // out of retries, so we can check at the bottom of the loop instead of the top.
            assert NUM_RETRIES > 0;

            // By default, our conservative strategy is to retry if the route permits it. Later we
            // may update this to unconditionally retry if we can definitely determine that the
            // server never saw the request.
            boolean retryRequest = (retryStrategy == RetryStrategy.SAFE_TO_RETRY);

            try {
                // In this block, any IOException will cause the request to be retried (up to a
                // total of NUM_RETRIES retries). RuntimeException (including DXAPIException)
                // instances are not caught and will immediately return control to the caller.

                // TODO: distinguish between errors during connection init and socket errors while
                // sending or receiving data. The former can always be retried, but the latter can
                // only be retried if the request is idempotent.
                HttpResponse response = httpclient.execute(request);

                int statusCode = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();

                if (statusCode == HttpStatus.SC_OK) {
                    // 200 OK
                    byte[] value = EntityUtils.toByteArray(entity);
                    int realLength = value.length;
                    if (entity.getContentLength() >= 0 && realLength != entity.getContentLength()) {
                        // Content length mismatch. Retry is possible (if the route permits it).
                        throw new IOException("Received response of " + realLength
                                + " bytes but Content-Length was " + entity.getContentLength());
                    } else if (parseResponse) {
                        JsonNode responseJson = null;
                        try {
                            responseJson = DXJSON.parseJson(new String(value, "UTF-8"));
                        } catch (JsonProcessingException e) {
                            if (entity.getContentLength() < 0) {
                                // content-length was not provided, and the JSON could not be
                                // parsed. Retry (if the route permits it) since this is probably
                                // just a streaming request that encountered a transient error.
                                throw new IOException(
                                        "Content-length was not provided and the response JSON could not be parsed.");
                            }
                            // This is probably a real problem (the request
                            // is complete but doesn't parse), so avoid
                            // masking it as an IOException (which is
                            // rethrown as DXHTTPException below). If it
                            // comes up frequently we can revisit how these
                            // should be handled.
                            throw new RuntimeException(
                                    "Request is of the correct length but is unparseable", e);
                        } catch (IOException e) {
                            // TODO: characterize what kinds of errors
                            // DXJSON.parseJson can emit, determine how we can
                            // get here and what to do about it.
                            throw new RuntimeException(e);
                        }
                        return new ParsedResponse(null, responseJson);
                    } else {
                        return new ParsedResponse(new String(value, Charset.forName("UTF-8")), null);
                    }
                } else if (statusCode < 500) {
                    // 4xx errors should be considered not recoverable.
                    String responseStr = EntityUtils.toString(entity);
                    String errorType = null;
                    String errorMessage = responseStr;
                    try {
                        JsonNode responseJson = DXJSON.parseJson(responseStr);
                        JsonNode errorField = responseJson.get("error");
                        if (errorField != null) {
                            JsonNode typeField = errorField.get("type");
                            if (typeField != null) {
                                errorType = typeField.asText();
                            }
                            JsonNode messageField = errorField.get("message");
                            if (messageField != null) {
                                errorMessage = messageField.asText();
                            }
                        }
                    } catch (IOException e) {
                        // Just fall back to reproducing the entire response
                        // body.
                    }

                    throw DXAPIException.getInstance(errorType, errorMessage, statusCode);
                } else {
                    // 500 InternalError should get retried unconditionally
                    retryRequest = true;
                    if (statusCode == 503) {
                        int retryAfterSeconds = 60;
                        Header retryAfterHeader = response.getFirstHeader("retry-after");
                        // Consume the response to avoid leaking resources
                        EntityUtils.consume(entity);
                        if (retryAfterHeader != null) {
                            try {
                                retryAfterSeconds = Integer.parseInt(retryAfterHeader.getValue());
                            } catch (NumberFormatException e) {
                                // Just fall back to the default
                            }
                        }
                        throw new ServiceUnavailableException(retryAfterSeconds);
                    }
                    throw new IOException(EntityUtils.toString(entity));
                }
            } catch (ServiceUnavailableException e) {
                // Retries due to 503 Service Unavailable and Retry-After do NOT count against the
                // allowed number of retries.
                int secondsToWait = e.secondsToWaitForRetry;
                System.err.println("POST " + resource + ": 503 Service Unavailable, waiting for "
                        + Integer.toString(secondsToWait) + " seconds");
                sleep(secondsToWait);
                continue;
            } catch (IOException e) {
                // Note, this catches both exceptions directly thrown from httpclient.execute (e.g.
                // no connectivity to server) and exceptions thrown by our code above after parsing
                // the response.
                System.err.println(errorMessage("POST", resource, e.toString(), timeoutSeconds,
                        attempts + 1, NUM_RETRIES));
                if (attempts == NUM_RETRIES || !retryRequest) {
                    throw new DXHTTPException(e);
                }
            }

            assert attempts < NUM_RETRIES;
            assert retryRequest;

            attempts++;

            // The number of failed attempts is now no more than NUM_RETRIES, and the total number
            // of attempts allowed is NUM_RETRIES + 1 (the first attempt, plus up to NUM_RETRIES
            // retries). So there is at least one more retry left; sleep before we retry.
            assert attempts <= NUM_RETRIES;

            sleep(timeoutSeconds);
            timeoutSeconds *= 2;
        }
    }
}
