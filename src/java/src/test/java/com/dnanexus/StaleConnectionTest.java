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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.protocol.BasicHttpContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link DXEnvironment} never reuses a pooled connection that a middlebox has
 * already discarded, without needing a real NAT gateway or a multi-minute sleep.
 *
 * <p>
 * The tests drive the real {@link DXEnvironment} and {@link DXHTTPRequest} stack against a local
 * stub server that emulates the two ways an idle connection dies:
 * </p>
 * <ul>
 * <li><b>NAT-style.</b> A NAT gateway tells the client nothing when it evicts a flow. Nothing
 * arrives, so the client's socket stays ESTABLISHED and looks perfectly healthy to httpclient's
 * stale check; the RST is produced only in reaction to the client's next write. The stub
 * reproduces exactly that: it stays silent until the client sends bytes, then answers with an
 * RST ({@code setSoLinger(true, 0)} before {@code close()}) rather than a FIN. Only the idle cap
 * can prevent this one.</li>
 * <li><b>Load-balancer-style.</b> An ALB or nginx idle timeout closes cleanly, so the FIN reaches
 * the client while it is still idle. That makes the dead socket detectable before reuse, which is
 * what httpclient's {@code validateAfterInactivity} is for.</li>
 * </ul>
 *
 * <p>
 * Eviction is per-flow, as a real gateway behaves: connections opened after the eviction event
 * get a fresh translation entry and work normally. Each test therefore asserts on whether the
 * pool <i>handed out</i> a dead connection, which is the policy under test, rather than on
 * whether an exception happened to surface -- the latter also depends on httpclient's internal
 * retry handler and would make these tests racy.
 * </p>
 */
public class StaleConnectionTest {

    /** How long the client is left idle before its second request, in milliseconds. */
    private static final long IDLE_MILLIS = 2500;

    /** An idle cap comfortably below {@link #IDLE_MILLIS}, so the cap is exceeded. */
    private static final int CAP_EXCEEDED_SECONDS = 1;

    /** An idle cap comfortably above {@link #IDLE_MILLIS}, so the cap is not exceeded. */
    private static final int CAP_NOT_EXCEEDED_SECONDS = 60;

    private NatStubServer stub;

    @Before
    public void startStub() throws IOException {
        stub = new NatStubServer();
        stub.start();
    }

    @After
    public void stopStub() {
        if (stub != null) {
            stub.close();
        }
    }

    /**
     * Builds an environment pointed at the stub server.
     *
     * <p>
     * Builder setters win over the config file and DX_* environment variables, so this is
     * unaffected by whatever the developer running the test has configured -- including any
     * HTTP_PROXY, which is explicitly cleared so requests go straight to the stub.
     * </p>
     */
    private DXEnvironment envWithIdleCap(int maxIdleSeconds) {
        return DXEnvironment.Builder.fromDefaults()
                .setApiserverProtocol("http")
                .setApiserverHost("127.0.0.1")
                .setApiserverPort(stub.getPort())
                .setBearerToken("dummy-token-not-used-by-the-stub")
                .setHttpProxy(null)
                .setConnectionMaxIdleSeconds(maxIdleSeconds)
                .setSocketTimeout(5000)
                .setConnectionTimeout(5000)
                .build();
    }

    /**
     * Makes a request, has the stub evict the flow, waits, then makes a second request.
     *
     * @return the exception the second request threw, or null if it succeeded
     */
    private Exception requestEvictWaitRequest(DXEnvironment env) throws Exception {
        DXHTTPRequest req = new DXHTTPRequest(env);
        // UNSAFE_TO_RETRY: DXHTTPRequest's own retry loop must not paper over the failure.
        req.request("/system/whoami", DXJSON.parseJson("{}"),
                DXHTTPRequest.RetryStrategy.UNSAFE_TO_RETRY);
        stub.evictIdleFlows();
        Thread.sleep(IDLE_MILLIS);
        try {
            req.request("/system/whoami", DXJSON.parseJson("{}"),
                    DXHTTPRequest.RetryStrategy.UNSAFE_TO_RETRY);
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    /**
     * The keep-alive cap must bound idle reuse while still deferring to a stricter server.
     */
    @Test
    public void testKeepAliveCapBoundsIdleReuse() {
        ConnectionKeepAliveStrategy strategy = DXEnvironment.cappedKeepAliveStrategy(45000);
        BasicHttpContext context = new BasicHttpContext();
        Assert.assertEquals("cap applies when the server advertises nothing",
                45000, strategy.getKeepAliveDuration(response(null), context));
        Assert.assertEquals("a stricter server keep-alive wins",
                10000, strategy.getKeepAliveDuration(response("timeout=10"), context));
        Assert.assertEquals("a longer server keep-alive is capped",
                45000, strategy.getKeepAliveDuration(response("timeout=600"), context));
        Assert.assertEquals("a Keep-Alive header without a timeout falls back to the cap",
                45000, strategy.getKeepAliveDuration(response("max=5"), context));
    }

    /**
     * A cap of zero would make the pool ignore idle time entirely rather than recycle eagerly
     * (httpclient treats a non-positive keep-alive as "no idle expiry"), so it must be rejected
     * rather than silently disabling the protection this class exists to verify.
     */
    @Test
    public void testNonPositiveIdleCapIsRejected() {
        for (int invalid : new int[] {0, -1}) {
            try {
                DXEnvironment.Builder.fromDefaults().setConnectionMaxIdleSeconds(invalid);
                Assert.fail("expected setConnectionMaxIdleSeconds(" + invalid + ") to be rejected");
            } catch (IllegalArgumentException expected) {
                // expected
            }
        }
    }

    /**
     * The shipped default must stay below the idle timeout of the middleboxes it protects
     * against. Raising it past an ALB's default 60s idle timeout would reintroduce the bug.
     */
    @Test
    public void testDefaultIdleCapIsConservative() {
        Assert.assertTrue("default idle cap must stay under a 60s load balancer idle timeout",
                DXEnvironment.DEFAULT_CONNECTION_MAX_IDLE_SECONDS < 60);
        Assert.assertTrue("default idle cap must be positive",
                DXEnvironment.DEFAULT_CONNECTION_MAX_IDLE_SECONDS > 0);
    }

    /**
     * The regression test proper: with the idle cap exceeded, a NAT-dropped connection must not
     * be handed out, so the request succeeds on a freshly opened connection.
     */
    @Test
    public void testNatDroppedConnectionIsNotReusedOnceIdleCapExceeded() throws Exception {
        DXEnvironment env = envWithIdleCap(CAP_EXCEEDED_SECONDS);
        try {
            Exception failure = requestEvictWaitRequest(env);
            Assert.assertNull("request should have succeeded on a fresh connection, but threw: "
                    + failure, failure);
            Assert.assertEquals("the dead connection must not have been handed out",
                    0, stub.getRequestsOnEvictedFlows());
            Assert.assertEquals("a replacement connection should have been opened",
                    2, stub.getConnectionsAccepted());
        } finally {
            env.close();
        }
    }

    /**
     * Negative control. With the same setup but the idle cap not yet exceeded, the pool does
     * reuse the dead connection -- confirming the previous test passes because of the cap and
     * not because the simulated drop failed to happen.
     *
     * <p>
     * This also demonstrates why the cap is needed at all: httpclient's
     * {@code validateAfterInactivity} check runs here (the connection has been idle longer than
     * its 2s default) and cannot detect the drop, because a NAT gateway sends nothing for the
     * check to notice.
     * </p>
     */
    @Test
    public void testNatDroppedConnectionIsReusedWithinIdleCap() throws Exception {
        DXEnvironment env = envWithIdleCap(CAP_NOT_EXCEEDED_SECONDS);
        try {
            requestEvictWaitRequest(env);
            Assert.assertEquals("the dead connection should have been handed out",
                    1, stub.getRequestsOnEvictedFlows());
        } finally {
            env.close();
        }
    }

    /**
     * When the peer closes cleanly instead, the FIN arrives while the client is idle and
     * httpclient's stale check replaces the connection on its own -- no idle cap involved. This
     * is why {@code validateAfterInactivity} is left at its default in
     * {@link DXEnvironment}: it covers the half of the problem the idle cap cannot see.
     */
    @Test
    public void testServerFinIsDetectedByStaleCheck() throws Exception {
        stub.setEvictWithFin(true);
        DXEnvironment env = envWithIdleCap(CAP_NOT_EXCEEDED_SECONDS);
        try {
            Exception failure = requestEvictWaitRequest(env);
            Assert.assertNull("stale check should have replaced the connection, but threw: "
                    + failure, failure);
            Assert.assertEquals("a replacement connection should have been opened",
                    2, stub.getConnectionsAccepted());
        } finally {
            env.close();
        }
    }

    private static HttpResponse response(String keepAliveHeader) {
        BasicHttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
        if (keepAliveHeader != null) {
            response.addHeader("Keep-Alive", keepAliveHeader);
        }
        return response;
    }

    /**
     * A keep-alive HTTP server whose connections can be "evicted" the way a NAT gateway evicts
     * an idle flow: silently, and answering only the client's next write, with an RST.
     */
    private static class NatStubServer {

        private static final byte[] BODY = "{}".getBytes();

        /** Poll interval on an idle connection, so eviction is noticed between requests. */
        private static final int IDLE_POLL_MILLIS = 100;

        private final List<AtomicBoolean> liveFlows = new ArrayList<AtomicBoolean>();
        private final AtomicInteger connectionsAccepted = new AtomicInteger();
        private final AtomicInteger requestsOnEvictedFlows = new AtomicInteger();
        private volatile boolean evictWithFin = false;
        private ServerSocket serverSocket;

        void start() throws IOException {
            serverSocket = new ServerSocket(0);
            Thread acceptor = new Thread(new Runnable() {
                @Override
                public void run() {
                    acceptLoop();
                }
            }, "nat-stub-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int getPort() {
            return serverSocket.getLocalPort();
        }

        int getConnectionsAccepted() {
            return connectionsAccepted.get();
        }

        /** Number of requests that arrived on a flow the gateway had already evicted. */
        int getRequestsOnEvictedFlows() {
            return requestsOnEvictedFlows.get();
        }

        /** Close evicted flows with a FIN (load balancer) rather than an RST (NAT gateway). */
        void setEvictWithFin(boolean evictWithFin) {
            this.evictWithFin = evictWithFin;
        }

        /**
         * Emulates the gateway dropping every flow that is currently open. Flows opened after
         * this point are unaffected, as with a real gateway.
         */
        void evictIdleFlows() {
            synchronized (liveFlows) {
                for (AtomicBoolean flow : liveFlows) {
                    flow.set(true);
                }
            }
        }

        void close() {
            try {
                serverSocket.close();
            } catch (IOException e) {
                // nothing useful to do while tearing down
            }
        }

        private void acceptLoop() {
            try {
                while (true) {
                    final Socket socket = serverSocket.accept();
                    connectionsAccepted.incrementAndGet();
                    socket.setTcpNoDelay(true);
                    final AtomicBoolean evicted = new AtomicBoolean(false);
                    synchronized (liveFlows) {
                        liveFlows.add(evicted);
                    }
                    Thread worker = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                serve(socket, evicted);
                            } finally {
                                synchronized (liveFlows) {
                                    liveFlows.remove(evicted);
                                }
                            }
                        }
                    }, "nat-stub-worker");
                    worker.setDaemon(true);
                    worker.start();
                }
            } catch (IOException e) {
                // server socket closed during teardown
            }
        }

        private void serve(Socket socket, AtomicBoolean evicted) {
            try {
                socket.setSoTimeout(IDLE_POLL_MILLIS);
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                while (true) {
                    if (evicted.get() && evictWithFin) {
                        // Load balancer idle timeout: close cleanly, so the FIN reaches the
                        // client while it is still idle and its stale check can see it.
                        socket.close();
                        return;
                    }
                    int firstByte;
                    try {
                        // Only this read may time out; a request always arrives as one burst.
                        firstByte = in.read();
                    } catch (SocketTimeoutException stillIdle) {
                        continue;
                    }
                    if (firstByte < 0) {
                        return;    // clean EOF: the client hung up, or its pool replaced us
                    }
                    socket.setSoTimeout(5000);
                    if (!readRestOfRequest(in, firstByte)) {
                        return;
                    }
                    socket.setSoTimeout(IDLE_POLL_MILLIS);
                    if (evicted.get()) {
                        // NAT gateway: the request hits a gateway with no translation entry for
                        // this flow, so it answers with an RST. setSoLinger(true, 0) makes
                        // close() emit an RST instead of a FIN.
                        requestsOnEvictedFlows.incrementAndGet();
                        socket.setSoLinger(true, 0);
                        socket.close();
                        return;
                    }
                    out.write(("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                            + "Content-Length: " + BODY.length + "\r\n\r\n").getBytes());
                    out.write(BODY);
                    out.flush();
                }
            } catch (IOException e) {
                // expected once we reset the connection, or when the client disconnects
            }
        }

        /**
         * Consumes the rest of a request: headers through the blank line, then a body of
         * Content-Length bytes.
         *
         * @param firstByte the first byte of the request, already read off the stream
         *
         * @return false on unexpected end of stream
         */
        private static boolean readRestOfRequest(InputStream in, int firstByte) throws IOException {
            StringBuilder head = new StringBuilder();
            head.append((char) firstByte);
            int newlines = 0;
            while (newlines < 2) {
                int c = in.read();
                if (c < 0) {
                    return false;
                }
                head.append((char) c);
                if (c == '\n') {
                    newlines++;
                } else if (c != '\r') {
                    newlines = 0;
                }
            }
            int contentLength = 0;
            for (String line : head.toString().split("\r\n")) {
                if (line.toLowerCase().startsWith("content-length:")) {
                    contentLength = Integer.parseInt(line.substring("content-length:".length()).trim());
                }
            }
            for (int i = 0; i < contentLength; i++) {
                if (in.read() < 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
