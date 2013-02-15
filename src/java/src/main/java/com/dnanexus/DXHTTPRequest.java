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

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import org.apache.http.*;
import org.apache.http.client.*;
import org.apache.http.util.*;
import org.apache.http.entity.*;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.client.ClientProtocolException;
import java.io.*;
import org.apache.commons.io.IOUtils;

public class DXHTTPRequest {
    private final JsonNode securityContext;
    private final String apiserver;
    private final DefaultHttpClient httpclient;
    private final JsonFactory dxJsonFactory;

    private static int NUM_RETRIES = 5;

    private static DXEnvironment env = DXEnvironment.create();

    public DXHTTPRequest() throws Exception {
        this.securityContext = env.getSecurityContext();
        this.apiserver = env.getApiserverPath();
        this.httpclient = new DefaultHttpClient();
        this.dxJsonFactory = new MappingJsonFactory();
    }

    private String errorMessage(String method, String resource, String errorString,
                                int retryWait, int nextRetryNum, int maxRetries) {
        String baseError = method + " " + resource + ": " + errorString + ".";
        if (nextRetryNum <= maxRetries) {
            return baseError + "  Waiting " + retryWait + " seconds before retry " + nextRetryNum
                + " of " + maxRetries;
        } else {
            return baseError;
        }
    }

    public String request(String resource, String data) throws Exception {
        HttpPost request = new HttpPost(apiserver + resource);

        request.setHeader("Content-Type", "application/json");
        request.setHeader("Authorization", securityContext.get("auth_token_type").textValue()
                          + " " + securityContext.get("auth_token").textValue());
        request.setEntity(new StringEntity(data));

        // Retry with exponential backoff
        int timeout = 1;

        for (int i = 0; i <= NUM_RETRIES; i++) {
            HttpResponse response = null;
            boolean okToRetry = false;

            try {
                response = httpclient.execute(request);
            } catch (ClientProtocolException e) {
                System.err.println(errorMessage("POST", resource, e.toString(), timeout, i + 1,
                                                NUM_RETRIES));
            } catch (IOException e) {
                System.err.println(errorMessage("POST", resource, e.toString(), timeout, i + 1,
                                                NUM_RETRIES));
            }

            if (response != null) {
                int statusCode = response.getStatusLine().getStatusCode();

                HttpEntity entity = response.getEntity();

                if (statusCode == HttpStatus.SC_OK) {
                    // 200 OK

                    String value = EntityUtils.toString(entity);
                    // Having to re-encode the string into UTF-8 is kind of
                    // crummy, but that's what we'll do to verify the
                    // Content-Length.
                    //
                    // TODO: compute the UTF-8 encoded length more efficiently,
                    // or find a way to make the HTTP stack verify the length
                    // itself.
                    int realLength = value.getBytes("UTF-8").length;
                    if (entity.getContentLength() >= 0 && realLength != entity.getContentLength()) {
                        String errorStr = "Received response of " + realLength
                            + " bytes but Content-Length was " + entity.getContentLength();
                        System.err.println(errorMessage("POST", resource, errorStr, timeout, i + 1,
                                                        NUM_RETRIES));
                    } else {
                        return value;
                    }
                } else {
                    // Non-200 status codes.

                    // 500 InternalError should get retried. 4xx errors should
                    // be considered not recoverable.
                    if (statusCode < 500) {
                        throw new Exception(EntityUtils.toString(entity));
                    } else {
                        System.err.println(errorMessage("POST", resource, EntityUtils.toString(entity),
                                                        timeout, i + 1, NUM_RETRIES));
                    }
                }
            }

            if (i < NUM_RETRIES) {
                Thread.sleep(timeout * 1000);
                timeout *= 2;
            }
        }

        throw new Exception("POST " + resource + " failed");
    }

    public JsonNode request(String resource, JsonNode data) throws Exception {
        String dataAsString = data.toString();
        String response = this.request(resource, dataAsString);
        JsonNode root = dxJsonFactory.createJsonParser(response).readValueAsTree();
        return root;
    }
}
