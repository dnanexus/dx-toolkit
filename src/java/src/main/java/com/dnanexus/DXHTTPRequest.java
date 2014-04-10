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
    private final JsonNode securityContext;
    private final String apiserver;
    private final DefaultHttpClient httpclient;

    private static final int NUM_RETRIES = 5;

    private static final DXEnvironment defaultEnv = DXEnvironment.create();

    private static final String USER_AGENT = DXUserAgent.getUserAgent();

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

    private static String errorMessage(String method, String resource, String errorString, int retryWait,
            int nextRetryNum, int maxRetries) {
        String baseError = method + " " + resource + ": " + errorString + ".";
        if (nextRetryNum <= maxRetries) {
            return baseError + "  Waiting " + retryWait + " seconds before retry " + nextRetryNum
                    + " of " + maxRetries;
        }
        return baseError;
    }

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
     * Issues a request against the specified resource and returns either the
     * text of the response or the parsed JSON of the response (depending on
     * whether parseResponse is set).
     *
     * @throws DXAPIException
     *             If the server returns a complete response with an HTTP status
     *             code other than 200 (OK).
     * @throws DXHTTPException
     *             If an error occurs while making the HTTP request or obtaining
     *             the response (includes HTTP protocol errors).
     */
    private ParsedResponse requestImpl(String resource, String data, boolean parseResponse) {
        HttpPost request = new HttpPost(apiserver + resource);

        request.setHeader("Content-Type", "application/json");
        request.setHeader("Authorization", securityContext.get("auth_token_type").textValue()
                          + " " + securityContext.get("auth_token").textValue());
        request.setEntity(new StringEntity(data, Charset.forName("UTF-8")));

        // Retry with exponential backoff
        int timeout = 1;

        for (int i = 0; i <= NUM_RETRIES; i++) {
            try {
                // In this block, any IOException will cause the request to be
                // retried (up to a total of 5 retries). RuntimeException
                // (including DXAPIException) are not caught and will
                // immediately return control to the caller.

                HttpResponse response = httpclient.execute(request);

                int statusCode = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();

                if (statusCode == HttpStatus.SC_OK) {
                    // 200 OK
                    byte[] value = EntityUtils.toByteArray(entity);
                    int realLength = value.length;
                    if (entity.getContentLength() >= 0 && realLength != entity.getContentLength()) {
                        // Content length mismatch.
                        throw new IOException("Received response of " + realLength + " bytes but Content-Length was "
                                + entity.getContentLength());
                    } else if (parseResponse) {
                        JsonNode responseJson = null;
                        try {
                            responseJson = DXJSON.parseJson(new String(value, "UTF-8"));
                        } catch (JsonProcessingException e) {
                            if (entity.getContentLength() < 0) {
                                // content-length was not provided, and the
                                // JSON could not be parsed. Retry since this
                                // is a streaming request from the server that
                                // probably just encountered a transient error.
                                // Retry the request (unless we've exceeded the
                                // maximum number of retries)
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
                    // 500 InternalError should get retried
                    throw new IOException(EntityUtils.toString(entity));
                }

                // We should never fall through to here: the request should
                // have succeeded (and returned) or thrown an exception (caught
                // below) by now.

            } catch (IOException e) {
                System.err.println(errorMessage("POST", resource, e.toString(), timeout, i + 1, NUM_RETRIES));
                if (i == NUM_RETRIES) {
                    throw new DXHTTPException(e);
                }
            }

            if (i < NUM_RETRIES) {
                try {
                    Thread.sleep(timeout * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                timeout *= 2;
            }
        }

        // We should never get here.
        throw new AssertionError("Exceeded max number of retries without throwing an error");
    }

    /**
     * Issues a request against the specified resource and returns the result as
     * a String.
     *
     * @throws DXAPIException
     *             If the server returns a complete response with an HTTP status
     *             code other than 200 (OK).
     * @throws DXHTTPException
     *             If an error occurs while making the HTTP request or obtaining
     *             the response (includes HTTP protocol errors).
     */
    public String request(String resource, String data) {
        return requestImpl(resource, data, false).responseText;
    }

    /**
     * Issues a request against the specified resource and returns the result as
     * a JSON object.
     *
     * @throws DXAPIException
     *             If the server returns a complete response with an HTTP status
     *             code other than 200 (OK).
     * @throws DXHTTPException
     *             If an error occurs while making the HTTP request or obtaining
     *             the response (includes HTTP protocol errors).
     */
    public JsonNode request(String resource, JsonNode data) {
        String dataAsString = data.toString();
        return requestImpl(resource, dataAsString, true).responseJson;
    }
}
