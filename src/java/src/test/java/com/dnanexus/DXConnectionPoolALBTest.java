package com.dnanexus;

import com.dnanexus.TestEnvironment.ConfigOption;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-end check that a pooled connection dropped by a real NAT gateway is not reused.
 *
 * Before the fix, DXEnvironment's pool bounded neither the idle time nor the total lifetime of a
 * pooled connection, so it would hand back a connection that had sat idle past the NAT gateway's
 * fixed timeout (~350s). The gateway drops such a flow without telling the client -- nothing
 * arrives, so the socket stays ESTABLISHED and looks healthy to httpclient's stale check -- and
 * answers the next request with an RST, which surfaces as a SocketException.
 *
 * After the fix, DXEnvironment caps how long a connection may sit idle before being reused
 * (see DXEnvironment.Builder#setConnectionMaxIdleSeconds). The pool checks that cap when a
 * connection is leased, so the stale connection is discarded and a fresh one opened instead.
 * Note this is a lease-time check, not a background eviction sweep.
 *
 * The ALB's own connection idle timeout (1800s) is not exercised here: NAT's fixed
 * 350s reset always fires first on the client-to-ALB path, so it is the binding
 * constraint for this scenario.
 *
 * This test only demonstrates anything from inside a network whose route to the API server
 * crosses such a gateway. Anywhere else the second request succeeds regardless of the fix, so
 * the test would pass without proving anything -- hence the DXTEST_NAT_IDLE_TIMEOUT gate. For
 * a check that runs anywhere and actually fails when the fix regresses, see
 * {@link StaleConnectionTest}, which simulates the drop against a local stub server.
 *
 * Usage:
 *   DXTEST_NAT_IDLE_TIMEOUT=1 mvn test -Dtest=DXConnectionPoolALBTest
 *
 * Environment:
 *   DX_APISERVER_HOST=stagingapi.dnanexus.com
 *   DX_SECURITY_CONTEXT='{"auth_token_type":"Bearer","auth_token":"TOKEN"}'
 */
public class DXConnectionPoolALBTest {

    private static void sleep(long seconds) {
        try {
            System.out.println("Sleeping " + seconds + "s...");
            for (long i = 0; i < seconds; i++) {
                Thread.sleep(1000);
                if ((i + 1) % 60 == 0) {
                    System.out.println((i + 1) + "s elapsed");
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test: Make request 1, sleep past NAT's fixed idle timeout (350s+), then make
     * request 2 reusing the pooled connection.
     *
     * Currently: second request gets Connection reset (no idle-eviction fix exists yet).
     * Once fixed: second request succeeds (stale connection evicted, fresh one created).
     */
    @Test
    public void testPooledConnectionAfterNATTimeout() throws Exception {
        if (!TestEnvironment.canRunTest(ConfigOption.NAT_IDLE_TIMEOUT)) {
            System.err.println("Skipping test that requires a NAT-fronted route to the API server");
            return;
        }

        // Use the configured API server (should be stagingapi.dnanexus.com)
        DXEnvironment env = DXEnvironment.create();
        System.out.println("Testing against: " + env.getApiserverPath()
                + " (idle cap " + env.getConnectionMaxIdleSeconds() + "s)");
        try {
            DXHTTPRequest req = new DXHTTPRequest(env);

            // Request 1: establish a pooled connection
            System.out.println("\n[1] Making first request to establish pooled connection...");
            JsonNode response1 = req.request("/system/findDataObjects", DXJSON.parseJson("{}"),
                    DXHTTPRequest.RetryStrategy.SAFE_TO_RETRY);
            Assert.assertTrue("First response should be valid JSON", response1.isObject());
            Assert.assertTrue("First response should have a results array",
                    response1.hasNonNull("results"));
            System.out.println("[1] Success: " + response1.get("results").size() + " results");

            // Sleep past NAT's fixed idle timeout (~350s). The gateway drops the flow and tells
            // the client nothing, so without an idle cap the pool still believes it is usable.
            sleep(365);

            // Request 2: attempt to reuse the pooled connection.
            // Uses UNSAFE_TO_RETRY so a stale-connection reset fails the test immediately instead
            // of being silently retried away by DXHTTPRequest's generic SAFE_TO_RETRY logic.
            System.out.println("\n[2] Making second request (reusing pooled connection)...");
            try {
                JsonNode response2 = req.request("/system/findDataObjects", DXJSON.parseJson("{}"),
                        DXHTTPRequest.RetryStrategy.UNSAFE_TO_RETRY);
                Assert.assertTrue("Second response should be valid JSON", response2.isObject());
                System.out.println("[2] Success");
                System.out.println("\nRESULT: OK — stale connection was not reused");
            } catch (Exception e) {
                System.out.println("[2] FAILED: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                System.out.println("\nRESULT: FAILURE — stale connection was reused and reset");
                Assert.fail("Stale pooled connection was reused and reset instead of being replaced: "
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        } finally {
            env.close();
        }
    }
}
