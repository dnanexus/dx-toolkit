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

import org.junit.Assert;
import org.junit.Test;

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.dnanexus.exceptions.InvalidAuthenticationException;
import com.dnanexus.exceptions.InvalidInputException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests for DXHTTPRequest and DXEnvironment.
 */
public class DXHTTPRequestTest {

    @JsonInclude(Include.NON_NULL)
    private static class ComeBackLaterRequest {
        @JsonProperty
        private final Long waitUntil;
        @JsonProperty
        private final boolean setRetryAfter;

        public ComeBackLaterRequest() {
            this.waitUntil = null;
            this.setRetryAfter = true;
        }

        public ComeBackLaterRequest(long waitUntil) {
            this.waitUntil = waitUntil;
            this.setRetryAfter = true;
        }

        public ComeBackLaterRequest(long waitUntil, boolean setRetryAfter) {
            this.waitUntil = waitUntil;
            this.setRetryAfter = setRetryAfter;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ComeBackLaterResponse {
        @JsonProperty
        private long currentTime;
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    private long getServerTime() {
        return DXJSON.safeTreeToValue(new DXHTTPRequest().request("/system/comeBackLater",
                mapper.valueToTree(new ComeBackLaterRequest()), RetryStrategy.SAFE_TO_RETRY),
                ComeBackLaterResponse.class).currentTime;
    }

    /**
     * Tests basic use of the API.
     *
     * @throws IOException
     */
    @Test
    public void testDXAPI() throws IOException {
        DXHTTPRequest c = new DXHTTPRequest();
        JsonNode responseJson =
                c.request("/system/findDataObjects", DXJSON.parseJson("{}"),
                        RetryStrategy.SAFE_TO_RETRY);
        Assert.assertEquals(responseJson.isObject(), true);

        // System.out.println(responseJson);

        String responseText =
                c.request("/system/findDataObjects", "{}", RetryStrategy.SAFE_TO_RETRY);
        Assert.assertEquals(responseText.substring(0, 1), "{");

        // Tests deserialization of InvalidInput
        DXHTTPRequest c2 = new DXHTTPRequest();
        try {
            c2.request("/system/findDataObjects",
                    DXJSON.parseJson("{\"state\": {\"invalid\": \"oops\"}}"),
                    RetryStrategy.SAFE_TO_RETRY);
            Assert.fail("Expected findDataObjects to fail with InvalidInput");
        } catch (InvalidInputException e) {
            // Error message should be something like
            // "expected key \"state\" of input to be a string"
            Assert.assertTrue(e.toString().contains("key \"state\""));
            Assert.assertEquals(422, e.getStatusCode());
        }
    }

    /**
     * Tests use of the API with a custom environment.
     *
     * @throws IOException
     */
    @Test
    public void testDXAPICustomEnvironment() throws IOException {
        DXEnvironment env = DXEnvironment.create();
        DXHTTPRequest c = new DXHTTPRequest(env);
        JsonNode responseJson =
                c.request("/system/findDataObjects", DXJSON.parseJson("{}"),
                        RetryStrategy.SAFE_TO_RETRY);
        Assert.assertEquals(responseJson.isObject(), true);

        // System.out.println(responseJson);

        String responseText =
                c.request("/system/findDataObjects", "{}", RetryStrategy.SAFE_TO_RETRY);
        Assert.assertEquals(responseText.substring(0, 1), "{");

        // Tests deserialization of InvalidAuthentication
        env = DXEnvironment.Builder.fromDefaults().setBearerToken("BOGUS").build();
        DXHTTPRequest c2 = new DXHTTPRequest(env);
        try {
            c2.request("/system/findDataObjects", DXJSON.parseJson("{}"),
                    RetryStrategy.SAFE_TO_RETRY);
            Assert.fail("Expected findDataObjects to fail with InvalidAuthentication");
        } catch (InvalidAuthenticationException e) {
            // Error message should be something like
            // "the token could not be found"
            Assert.assertTrue(e.toString().contains("token"));
            Assert.assertEquals(401, e.getStatusCode());
        }
    }

    /**
     * Tests creating DXEnvironments.
     */
    @Test
    public void testDXEnvironment() {
        DXEnvironment env = DXEnvironment.create();

        // Using fromEnvironment gives us complete control over the environment that will be
        // created, regardless of any environment variable settings.
        DXEnvironment env1 =
                DXEnvironment.Builder.fromEnvironment(env).setApiserverHost("example.dnanexus.com")
                        .setApiserverPort(31337).setApiserverProtocol("https").build();

        DXEnvironment env2 = DXEnvironment.Builder.fromEnvironment(env1).build();

        Assert.assertEquals("https://example.dnanexus.com:31337", env2.getApiserverPath());
        Assert.assertEquals(env1.getApiserverPath(), env2.getApiserverPath());

        DXEnvironment envWithDifferentToken =
                DXEnvironment.Builder.fromEnvironment(env2).setBearerToken("abcdef").build();
        Assert.assertEquals(
                DXJSON.getObjectBuilder().put("auth_token_type", "Bearer")
                        .put("auth_token", "abcdef").build(),
                envWithDifferentToken.getSecurityContextJson());

    }

    /**
     * Tests retry logic following 503 Service Unavailable errors.
     */
    @Test
    public void testRetryAfterServiceUnavailable() {
        // Do this weird dance here in case there is clock skew between client
        // and server.
        long startTime = System.currentTimeMillis();
        long serverTime = getServerTime();
        new DXHTTPRequest().request("/system/comeBackLater",
                mapper.valueToTree(new ComeBackLaterRequest(serverTime + 8000)),
                RetryStrategy.SAFE_TO_RETRY);
        long timeElapsed = System.currentTimeMillis() - startTime;
        Assert.assertTrue(8000 <= timeElapsed);
        Assert.assertTrue(timeElapsed <= 16000);
    }

    /**
     * Retry logic: test that Retry-After requests do not count towards the max number of retries.
     */
    @Test
    public void testRetryAfterServiceUnavailableExceedingMaxRetries() {
        long startTime = System.currentTimeMillis();
        long serverTime = getServerTime();
        // Retry-After is 2s for the comeBackLater route and this time exceeds 7 tries * 2s
        new DXHTTPRequest().request("/system/comeBackLater",
                mapper.valueToTree(new ComeBackLaterRequest(serverTime + 20000)),
                RetryStrategy.SAFE_TO_RETRY);
        long timeElapsed = System.currentTimeMillis() - startTime;
        Assert.assertTrue(16000 <= timeElapsed);
        Assert.assertTrue(timeElapsed <= 30000);
    }

    /**
     * Retry logic: test that the default value of 60 seconds is used when no Retry-After header is
     * specified.
     */
    @Test
    public void testRetryAfterServiceUnavailableWithoutRetryAfter() {
        long startTime = System.currentTimeMillis();
        long serverTime = getServerTime();
        new DXHTTPRequest().request("/system/comeBackLater",
                mapper.valueToTree(new ComeBackLaterRequest(serverTime + 20000, false)),
                RetryStrategy.SAFE_TO_RETRY);
        long timeElapsed = System.currentTimeMillis() - startTime;
        Assert.assertTrue(50000 <= timeElapsed);
        Assert.assertTrue(timeElapsed <= 70000);
    }
}
