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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Verifies that {@link DXAPI} methods tolerate response properties that the caller's output class
 * does not declare, end to end through the real {@link DXEnvironment} and {@link DXHTTPRequest}
 * stack.
 *
 * <p>
 * This reproduces the case where the API server adds a field (here {@code readOnly} on
 * {@code /system/whoami}) and a caller deserializes into its own class that has no such field and
 * no {@code @JsonIgnoreProperties} annotation. A local stub server stands in for the API server,
 * so the test needs no credentials or network access.
 * </p>
 */
public class DXAPIUnknownPropertiesTest {

    /** A caller-defined response class that knows only about {@code id}, like a strict client. */
    public static class IdOnlyWhoamiResponse {
        public String id;
    }

    private HttpServer stub;

    @Before
    public void startStub() throws IOException {
        stub = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        stub.createContext("/system/whoami", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                InputStream in = exchange.getRequestBody();
                while (in.read() >= 0) {
                    // drain the request body
                }
                byte[] body = "{\"id\": \"user-test\", \"readOnly\": false}".getBytes("UTF-8");
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                OutputStream out = exchange.getResponseBody();
                out.write(body);
                out.close();
            }
        });
        stub.start();
    }

    @After
    public void stopStub() {
        if (stub != null) {
            stub.stop(0);
        }
    }

    /**
     * Builds an environment pointed at the stub server. Builder setters win over the config file
     * and DX_* environment variables, and the proxy is cleared so requests go straight to the stub.
     */
    private DXEnvironment stubEnv() {
        return DXEnvironment.Builder.fromDefaults()
                .setApiserverProtocol("http")
                .setApiserverHost("127.0.0.1")
                .setApiserverPort(stub.getAddress().getPort())
                .setBearerToken("dummy-token-not-used-by-the-stub")
                .setHttpProxy(null)
                .setSocketTimeout(5000)
                .setConnectionTimeout(5000)
                .build();
    }

    @Test
    public void testSystemWhoamiIgnoresUnknownResponseProperty() throws IOException {
        DXEnvironment env = stubEnv();
        try {
            IdOnlyWhoamiResponse response =
                    DXAPI.systemWhoami(IdOnlyWhoamiResponse.class, env);
            Assert.assertEquals("user-test", response.id);
        } finally {
            env.close();
        }
    }
}
