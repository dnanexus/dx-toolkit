package com.dnanexus;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration test reproducing stale pooled connection reuse against a NAT-fronted ALB.
 *
 * The client's connection pool (PoolingHttpClientConnectionManager in DXEnvironment) has no
 * idle-eviction policy, so it can keep and reuse a connection past the NAT gateway's fixed
 * idle timeout (~350s), after which the NAT silently drops it. Reusing that connection then
 * fails with a SocketException. This test currently fails on master, demonstrating the issue.
 * It is expected to pass once the pool proactively evicts connections idle longer than some
 * threshold below 350s.
 *
 * The ALB's own connection idle timeout (1800s) is not exercised here: NAT's fixed
 * 350s reset always fires first on the client-to-ALB path, so it is the binding
 * constraint for this scenario.
 *
 * Usage:
 *   mvn test -Dtest=DXConnectionPoolALBTest#testPooledConnectionAfterNATTimeout
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
        // Use the configured API server (should be stagingapi.dnanexus.com)
        DXEnvironment env = DXEnvironment.create();
        System.out.println("Testing against: " + env.getApiserverPath());

        DXHTTPRequest req = new DXHTTPRequest(env);

        // Request 1: establish a pooled connection
        System.out.println("\n[1] Making first request to establish pooled connection...");
        JsonNode response1 = req.request("/system/findDataObjects", DXJSON.parseJson("{}"),
                DXHTTPRequest.RetryStrategy.SAFE_TO_RETRY);
        Assert.assertTrue("First response should be valid JSON", response1.isObject());
        System.out.println("[1] Success: " + response1.get("results").size() + " results");

        // Sleep past NAT's fixed idle timeout (~350s): the server silently closes the
        // connection, but the client-side pool has no eviction policy and still holds it.
        sleep(365);

        // Request 2: attempt to reuse the pooled connection.
        // Uses UNSAFE_TO_RETRY so a stale-connection reset fails the test immediately instead
        // of being silently retried away by DXHTTPRequest's generic SAFE_TO_RETRY logic.
        System.out.println("\n[2] Making second request (reusing pooled connection)...");
        try {
            JsonNode response2 = req.request("/system/findDataObjects", DXJSON.parseJson("{}"),
                    DXHTTPRequest.RetryStrategy.UNSAFE_TO_RETRY);
            Assert.assertTrue("Second response should be valid JSON", response2.isObject());
            System.out.println("[2] Success: " + response2.get("results").size() + " results");
            System.out.println("\nRESULT: OK — stale connection was evicted before reuse");
        } catch (Exception e) {
            System.out.println("[2] FAILED: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("\nRESULT: FAILURE — stale connection was reused and reset");
            Assert.fail("Stale pooled connection was reused and reset instead of being evicted: "
                    + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
}
