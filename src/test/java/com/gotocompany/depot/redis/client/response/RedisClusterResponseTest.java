package com.gotocompany.depot.redis.client.response;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RedisClusterResponse}, the eagerly-evaluated response wrapper for cluster
 * commands that records success or failure at construction time.
 *
 * <p>Unlike the standalone variant these tests need no mocks: the three-argument constructor takes the
 * command, its result and the TTL result directly, while the single-argument constructor records a
 * failure message. They verify the failure flag and the combined status message in each case.</p>
 */
public class RedisClusterResponseTest {
    /**
     * Instance under test, constructed per scenario.
     */
    private RedisClusterResponse redisClusterResponse;

    /**
     * Verifies that the three-argument constructor reports a non-failed combined status message.
     *
     * <p>Given a {@link RedisClusterResponse} built from command {@code "SET"}, result
     * {@code "Success"} and TTL result {@code 1}, when its state is inspected, then it is not failed
     * and its message is {@code "SET: Success, TTL: UPDATED"}.</p>
     */
    @Test
    public void shouldReportWhenSuccess() {
        String response = "Success";
        Long ttlResponse = 1L;
        redisClusterResponse = new RedisClusterResponse("SET", response, ttlResponse);
        Assert.assertFalse(redisClusterResponse.isFailed());
        Assert.assertEquals("SET: Success, TTL: UPDATED", redisClusterResponse.getMessage());
    }

    /**
     * Verifies that the single-argument constructor records a failure.
     *
     * <p>Given a {@link RedisClusterResponse} built from the failure message {@code "Failed"}, when
     * its state is inspected, then it is failed and its message is {@code "Failed"}.</p>
     */
    @Test
    public void shouldReportWhenFailed() {
        redisClusterResponse = new RedisClusterResponse("Failed");
        Assert.assertTrue(redisClusterResponse.isFailed());
        Assert.assertEquals("Failed", redisClusterResponse.getMessage());
    }
}
