// Copyright (C) 2016 DNAnexus, Inc.
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

import com.dnanexus.exceptions.InternalErrorException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.*;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Tests for DXHTTPRequest against APIserver mock object
 */
public class DXHTTPRequestRetryTest {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static class WhoamiRequest {
        @JsonProperty
        private final boolean preauthenticated = true;
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    private final int API_SERVER_TCP_PORT = 8080;
    private Process apiServerMockProcess = null;
    private DXEnvironment env = null;

    @Before
    public void setUp() throws IOException, InterruptedException {
        Path apiMockPath = Paths.get(System.getProperty("user.dir"), "..", "python", "test", "mock_api");
        Path apiMockServer = Paths.get(apiMockPath.toString(), "apiserver_mock.py");
        Path apiMockServer503Handler = Paths.get(apiMockPath.toString(), "test_retry.py");
        apiServerMockProcess = Runtime.getRuntime().exec(apiMockServer + " " + apiMockServer503Handler + " " +
                Integer.toString(API_SERVER_TCP_PORT));
        //System.out.println("APIserver mock process is: " + apiServerMockProcess.toString());
        Thread.sleep(500);

        env = DXEnvironment.Builder.fromEnvironment(DXEnvironment.create()).setApiserverHost("localhost")
                        .setApiserverPort(API_SERVER_TCP_PORT).setApiserverProtocol("http").build();
    }

    @After
    public void tearDown() {
        apiServerMockProcess.destroy();
    }

    private void checkRetry(String testingMode) throws IOException, java.text.ParseException {
        HttpClient c = HttpClientBuilder.create().setUserAgent(DXUserAgent.getUserAgent()).build();
        HttpResponse response = c.execute(new HttpGet("http://localhost:" + Integer.toString(this.API_SERVER_TCP_PORT) +
                "/set_testing_mode/" + testingMode));

        final DXHTTPRequest req = new DXHTTPRequest(env);
        InternalErrorException requestException = null;
        try {
            req.request("/system/whoami", mapper.valueToTree(new WhoamiRequest()), DXHTTPRequest.RetryStrategy.SAFE_TO_RETRY);
        } catch (InternalErrorException e) {
            requestException = e;
        }

        HttpResponse statResponse = c.execute(new HttpGet("http://localhost:" + Integer.toString(this.API_SERVER_TCP_PORT) +
                "/stats"));
        BufferedReader rd = new BufferedReader(new InputStreamReader(statResponse.getEntity().getContent()));
        StringBuffer statBuffer = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            statBuffer.append(line);
        }
        JsonNode statJson = DXJSON.parseJson(statBuffer.toString());
        //System.out.println("POST request stat is: " + statJson.get("postRequests").toString() + ", node type is: " + statJson.get("postRequests").getNodeType().toString());
        Iterator<JsonNode> statIterator = statJson.get("postRequests").iterator();
        double prevItemTimestamp = 0.0;
        int i = 0;
        while (statIterator.hasNext()) {
            double itemTimestamp =  (double) DatatypeConverter.parseDateTime(statIterator.next().get("timestamp").asText()).getTimeInMillis() / 1000.0;
            if (prevItemTimestamp > 0.0) {
                double interval = itemTimestamp - prevItemTimestamp;
                if (testingMode.equals("503_retry_after")) {
                    Assert.assertTrue(((double) i) <= interval);
                    Assert.assertTrue(interval <= (((double) i) + 0.5));
                } else if ((testingMode.equals("503_mixed") && i == 3) || (testingMode.equals("mixed") && i == 4)) {
                    Assert.assertTrue(2.0 <= interval);
                    Assert.assertTrue(interval <= 2.5);
                } else if (testingMode.equals("503_mixed_limited")) {
                    if (i < 11) {
                        Assert.assertTrue(1.0 <= interval);
                        Assert.assertTrue(interval <= 1.5);
                    } else {
                        Assert.assertTrue(300.0 <= interval);
                        Assert.assertTrue(interval <= 600.5);
                    }
                } else {
                    Assert.assertTrue(Math.pow(2.0, (i - 1.0)) <= interval);
                    Assert.assertTrue(interval <= Math.pow(2.0, i) + 0.5);
                }
            }
            i++;
            prevItemTimestamp = itemTimestamp;
        }

        if (testingMode.equals("500_fail")) {
            Assert.assertNotEquals(null, requestException);
            Assert.assertTrue(requestException.toString().equals("DXAPIException: Maximum number of retries reached, or unsafe to retry"));
            Assert.assertEquals(7, i);
        } else if (testingMode.equals("503_mixed_limited")) {
            Assert.assertEquals(12, i);
        } else {
            Assert.assertEquals(5, i);
        }
    }

    /**
     * Tests randomized exponential backoff having 500
     */
    @Test
    public void test500() throws IOException, java.text.ParseException {
        checkRetry("500");
    }

    /**
     * Tests randomized exponential backoff having 500, which fails because max retry amount got hit
     */
    @Ignore("Could run too long, up to 2 minutes") @Test
    public void test500Fail() throws IOException, java.text.ParseException {
        checkRetry("500_fail");
    }

    /**
     * Tests randomized exponential backoff having 503 w/o 'Retry-After' header
     */
    @Test
    public void test503() throws IOException, java.text.ParseException {
        checkRetry("503");
    }

    /**
     * Tests randomized exponential backoff having 503 with 'Retry-After' header
     */
    @Test
    public void test503RetryAfter() throws IOException, java.text.ParseException {
        checkRetry("503_retry_after");
    }

    /**
     * Tests randomized exponential backoff having 503 with mixed 'Retry-After' header
     */
    @Test
    public void test503Mixed() throws IOException, java.text.ParseException {
        checkRetry("503_mixed");
    }

    /**
     * Tests randomized exponential backoff having 503 with mixed 'Retry-After' header
     * is limited to 600 seconds
     */
    @Ignore("Could run too long, up to 10 minutes") @Test
    public void test503MixedLimited() throws IOException, java.text.ParseException {
        checkRetry("503_mixed_limited");
    }

    /**
     * Tests randomized exponential backoff having 5xx
     */
    @Test
    public void testMixed() throws IOException, java.text.ParseException {
        checkRetry("mixed");
    }

}
