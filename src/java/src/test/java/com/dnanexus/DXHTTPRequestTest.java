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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.http.pool.ConnPoolControl;
import org.junit.Assert;
import org.junit.Test;

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.dnanexus.exceptions.InternalErrorException;
import com.dnanexus.exceptions.InvalidAuthenticationException;
import com.dnanexus.exceptions.InvalidInputException;
import com.dnanexus.exceptions.ServiceUnavailableException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

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

    @JsonInclude(Include.NON_NULL)
    private static class WhoamiRequest {
        @JsonProperty
        private final boolean preauthenticated = true;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class WhoamiResponse {
        @JsonProperty
        private String id;
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
     * Test that we don't exhaust file handles even when GC is slow to free up the DXHTTPRequest
     * objects. That is, we must be responsible for how much connections (sockets) we are using.
     * By limiting ourselves to a single {@link HttpClient} we should have {@link ConnPoolControl#getMaxTotal()}
     * connections (20 by default) at most.
     *
     * Note: this test doesn't seem to fail properly when the threads are unable to allocate more
     * file handles. Instead, it hangs, so a reasonable timeout on the test at the top level may be
     * sufficient to catch regressions. When the test passes, it takes about 13s on my machine.
     */
    @Test
    public void testRequestResourceLeakage() throws InterruptedException, ExecutionException {
        ExecutorService threadPool = Executors.newFixedThreadPool(200);

        // Hang on to each DXHTTPRequest. This is our proxy to simulate a system where GC is not
        // happening often enough to free up the file handles in a timely manner
        List<DXHTTPRequest> requests = Lists.newArrayList();
        List<Future<String>> futures = Lists.newArrayList();
        for (int i = 0; i < 5000; ++i) {
            final DXHTTPRequest req = new DXHTTPRequest();
            requests.add(req);
            Future<String> f = threadPool.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    // Same as the implementation of DXAPI.systemWhoami, except allows us to hold on
                    // to the DXHTTPRequest object being used.
                    WhoamiResponse response = DXJSON.safeTreeToValue(req.request("/system/whoami",
                            mapper.valueToTree(new WhoamiRequest()), RetryStrategy.SAFE_TO_RETRY),
                            WhoamiResponse.class);
                    return response.id;
                }
            });
            futures.add(f);
        }

        threadPool.shutdown();
        threadPool.awaitTermination(1000, TimeUnit.SECONDS);
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
        // Unpredictable system load or transient problems may cause the time
        // taken to exceed 60 seconds by an arbitrary amount of time. How to
        // test this?
        //
        // Assert.assertTrue(timeElapsed <= 70000);
    }

    /**
     * Tests retry logic is disabled following 503 Service Unavailable error.
     */
    @Test
    public void testRetryDisabledAfterServiceUnavailable() {
        // Create environment that disables retry logic with 503
        DXEnvironment env = DXEnvironment.Builder.fromDefaults().disableRetry().build();

        // Check that retry really is disabled
        Assert.assertTrue(env.isRetryDisabled());

        boolean thrown = false;
        long startTime = System.currentTimeMillis();
        long serverTime = getServerTime();
        try {
            new DXHTTPRequest(env).request("/system/comeBackLater",
                    mapper.valueToTree(new ComeBackLaterRequest(serverTime + 8000)),
                    RetryStrategy.SAFE_TO_RETRY);
        } catch (ServiceUnavailableException e) {
            thrown = true;
            long timeElapsed = System.currentTimeMillis() - startTime;
            Assert.assertTrue(timeElapsed < 2500);
        }

        Assert.assertTrue(thrown);
    }

    /**
     * Tests that disabling the retry logic does not change the behavior of a 4xx error.
     * @throws IOException
     */
    @Test
    public void testRetryDisabledDoesNotAffectError() throws IOException {
        // Create environment that disables retry logic with 4xx error
        DXEnvironment env = DXEnvironment.Builder.fromDefaults().disableRetry().setBearerToken("BOGUS").build();

        // Check that retry really is disabled
        Assert.assertTrue(env.isRetryDisabled());

        boolean thrown = false;
        long startTime = System.currentTimeMillis();

        // Tests deserialization of InvalidAuthentication
        DXHTTPRequest c = new DXHTTPRequest(env);
        try {
            c.request("/system/findDataObjects", DXJSON.parseJson("{}"),
                    RetryStrategy.SAFE_TO_RETRY);
            Assert.fail("Expected findDataObjects to fail with InvalidAuthentication");
        } catch (InvalidAuthenticationException e) {
            thrown = true;
            long timeElapsed = System.currentTimeMillis() - startTime;
            Assert.assertTrue(timeElapsed < 2500);

            // Error message should be something like
            // "the token could not be found"
            Assert.assertTrue(e.toString().contains("token"));
            Assert.assertEquals(401, e.getStatusCode());
        }

        Assert.assertTrue(thrown);
    }
}
