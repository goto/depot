package com.gotocompany.depot.redis.client.response;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RedisStandaloneResponse}, the deferred response wrapper that resolves a
 * pipelined Redis command and its TTL command once the pipeline has synced.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with mocked Jedis {@link Response} handles for the
 * command and the TTL result. They verify that {@link RedisStandaloneResponse#process()} reports
 * success with a combined status message when the deferred values resolve, and reports failure
 * carrying the exception text when a {@link JedisException} surfaces.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisStandaloneResponseTest {
    /**
     * Mocked deferred handle for the primary command's result.
     */
    @Mock
    private Response response;
    /**
     * Mocked deferred handle for the TTL command's result.
     */
    @Mock
    private Response ttlResponse;
    /**
     * Instance under test, constructed per scenario from the deferred handles.
     */
    private RedisStandaloneResponse redisResponse;

    /**
     * Verifies that a resolved response reports success with a combined status message.
     *
     * <p>Given the command handle resolving to {@code "Success response"} and the TTL handle to
     * {@code 1}, when {@link RedisStandaloneResponse#process()} is called, then the response is not
     * failed and its message is {@code "SET: Success response, TTL: UPDATED"}.</p>
     */
    @Test
    public void shouldReportNotFailedWhenJedisExceptionNotThrown() {
        when(response.get()).thenReturn("Success response");
        when(ttlResponse.get()).thenReturn(1L);
        redisResponse = new RedisStandaloneResponse("SET", response, ttlResponse);
        Assert.assertFalse(redisResponse.process().isFailed());
        Assert.assertEquals("SET: Success response, TTL: UPDATED", redisResponse.process().getMessage());
    }

    /**
     * Verifies that a {@link JedisException} during resolution marks the response as failed.
     *
     * <p>Given the command handle throwing a {@link JedisException} of {@code "Failed response"}, when
     * {@link RedisStandaloneResponse#process()} is called, then the response is failed and its message
     * is {@code "Failed response"}.</p>
     */
    @Test
    public void shouldReportFailedWhenJedisExceptionThrown() {
        when(response.get()).thenThrow(new JedisException("Failed response"));
        redisResponse = new RedisStandaloneResponse("SET", response, ttlResponse);
        Assert.assertTrue(redisResponse.process().isFailed());
        Assert.assertEquals("Failed response", redisResponse.process().getMessage());
    }
}
