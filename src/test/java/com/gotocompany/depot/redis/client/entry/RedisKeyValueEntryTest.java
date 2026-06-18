package com.gotocompany.depot.redis.client.entry;


import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.redis.ttl.DurationTtl;
import com.gotocompany.depot.redis.ttl.NoRedisTtl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RedisKeyValueEntry}, the {@link RedisEntry} that writes a key-value pair via
 * {@code SET} and optionally applies a TTL to the key.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked {@link Instrumentation} and mocked
 * Jedis {@link Pipeline} and {@link JedisCluster} targets. They cover both the cluster and standalone
 * execution paths across a no-op TTL ({@link NoRedisTtl}) and a duration TTL ({@link DurationTtl}),
 * asserting the combined status message, the debug logging of the written key and value, and the
 * failure handling when a {@link JedisException} is raised by the command or the TTL call. One test
 * also covers the {@link RedisKeyValueEntry#toString()} representation.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisKeyValueEntryTest {
    /**
     * Mocked instrumentation used to verify the debug logging of each write.
     */
    @Mock
    private Instrumentation instrumentation;
    /**
     * Mocked standalone pipeline used to capture the queued commands.
     */
    @Mock
    private Pipeline pipeline;
    /**
     * Mocked cluster connection used to capture the executed commands.
     */
    @Mock
    private JedisCluster jedisCluster;
    /**
     * Instance under test, created in {@link #setup()} for key {@code "test-key"} and value
     * {@code "test-value"}.
     */
    private RedisKeyValueEntry redisKeyValueEntry;

    /**
     * Creates the {@link RedisKeyValueEntry} under test for key {@code "test-key"} and value
     * {@code "test-value"}, wired with the mocked {@link Instrumentation}.
     */
    @Before
    public void setup() {
        redisKeyValueEntry = new RedisKeyValueEntry("test-key", "test-value", instrumentation);
    }

    /**
     * Verifies a successful cluster write with no TTL.
     *
     * <p>Given the cluster {@code SET} returning {@code "OK"} and a {@link NoRedisTtl}, when the entry
     * is sent to the cluster, then the response is not failed, the key and value are logged once at
     * debug level, and the message is {@code "SET: OK, TTL: NoOp"}.</p>
     */
    @Test
    public void shouldSentToRedisForCluster() {
        when(jedisCluster.set("test-key", "test-value")).thenReturn("OK");
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: NoOp", clusterResponse.getMessage());
    }

    /**
     * Verifies a successful cluster write whose duration TTL is applied.
     *
     * <p>Given the cluster {@code SET} returning {@code "OK"} and {@code EXPIRE} returning {@code 1}
     * for a {@link DurationTtl} of {@code 1000}, when the entry is sent to the cluster, then the
     * response is not failed and the message is {@code "SET: OK, TTL: UPDATED"}.</p>
     */
    @Test
    public void shouldSentToRedisForClusterWithTTL() {
        when(jedisCluster.set("test-key", "test-value")).thenReturn("OK");
        when(jedisCluster.expire("test-key", 1000)).thenReturn(1L);
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: UPDATED", clusterResponse.getMessage());
    }

    /**
     * Verifies a successful cluster write whose duration TTL is not applied.
     *
     * <p>Given the cluster {@code SET} returning {@code "OK"} and {@code EXPIRE} returning {@code 0}
     * for a {@link DurationTtl} of {@code 1000}, when the entry is sent to the cluster, then the
     * response is not failed and the message is {@code "SET: OK, TTL: NOT UPDATED"}.</p>
     */
    @Test
    public void shouldSentToRedisForClusterWithTTLNotUpdated() {
        when(jedisCluster.set("test-key", "test-value")).thenReturn("OK");
        when(jedisCluster.expire("test-key", 1000)).thenReturn(0L);
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertFalse(clusterResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: NOT UPDATED", clusterResponse.getMessage());
    }

    /**
     * Verifies that a {@link JedisException} from the cluster command is reported as a failure.
     *
     * <p>Given the cluster {@code SET} throwing a {@link JedisException} of
     * {@code "jedis error occurred"}, when the entry is sent to the cluster, then the response is
     * failed and the message is {@code "jedis error occurred"}.</p>
     */
    @Test
    public void shouldReportFailedForJedisExceptionForCluster() {
        when(jedisCluster.set("test-key", "test-value")).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new NoRedisTtl());
        Assert.assertTrue(clusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", clusterResponse.getMessage());
    }

    /**
     * Verifies that a {@link JedisException} from the cluster TTL call is reported as a failure.
     *
     * <p>Given the cluster {@code SET} succeeding but {@code EXPIRE} throwing a {@link JedisException}
     * for a {@link DurationTtl} of {@code 1000}, when the entry is sent to the cluster, then the
     * response is failed and the message is {@code "jedis error occurred"}.</p>
     */
    @Test
    public void shouldReportFailedForJedisExceptionFromTTLForCluster() {
        when(jedisCluster.set("test-key", "test-value")).thenReturn("OK");
        when(jedisCluster.expire("test-key", 1000)).thenThrow(new JedisException("jedis error occurred"));
        RedisClusterResponse clusterResponse = redisKeyValueEntry.send(jedisCluster, new DurationTtl(1000));
        Assert.assertTrue(clusterResponse.isFailed());
        Assert.assertEquals("jedis error occurred", clusterResponse.getMessage());
    }

    /**
     * Verifies the human-readable representation of the entry.
     *
     * <p>Given the entry for key {@code "test-key"} and value {@code "test-value"}, when
     * {@link RedisKeyValueEntry#toString()} is called, then it returns
     * {@code "RedisKeyValueEntry: Key test-key, Value test-value"}.</p>
     */
    @Test
    public void shouldGetEntryToString() {
        String expected = "RedisKeyValueEntry: Key test-key, Value test-value";
        Assert.assertEquals(expected, redisKeyValueEntry.toString());
    }


    /**
     * Verifies a successful standalone write with no TTL.
     *
     * <p>Given the pipelined {@code SET} resolving to {@code "OK"} and a {@link NoRedisTtl}, when the
     * entry is sent through the pipeline and the deferred response is processed, then it is not failed,
     * the key and value are logged once at debug level, and the message is
     * {@code "SET: OK, TTL: NoOp"}.</p>
     */
    @Test
    public void shouldSentToRedisForStandAlone() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn("OK");
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new NoRedisTtl());
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: NoOp", standaloneResponse.getMessage());
    }

    /**
     * Verifies a successful standalone write whose duration TTL is applied.
     *
     * <p>Given the pipelined {@code SET} resolving to {@code "OK"} and {@code EXPIRE} to {@code 1} for
     * a {@link DurationTtl} of {@code 1000}, when the entry is sent through the pipeline and processed,
     * then it is not failed and the message is {@code "SET: OK, TTL: UPDATED"}.</p>
     */
    @Test
    public void shouldSentToRedisForStandaloneWithTTL() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn("OK");
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenReturn(1L);
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: UPDATED", standaloneResponse.getMessage());
    }

    /**
     * Verifies a successful standalone write whose duration TTL is not applied.
     *
     * <p>Given the pipelined {@code SET} resolving to {@code "OK"} and {@code EXPIRE} to {@code 0} for
     * a {@link DurationTtl} of {@code 1000}, when the entry is sent through the pipeline and processed,
     * then it is not failed and the message is {@code "SET: OK, TTL: NOT UPDATED"}.</p>
     */
    @Test
    public void shouldSentToRedisForStandaloneWithTTLNotUpdated() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn("OK");
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenReturn(0L);
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertFalse(standaloneResponse.isFailed());
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
        Assert.assertEquals("SET: OK, TTL: NOT UPDATED", standaloneResponse.getMessage());
    }

    /**
     * Verifies that a {@link JedisException} from the pipelined command is reported as a failure.
     *
     * <p>Given the pipelined {@code SET} whose deferred result throws a {@link JedisException} of
     * {@code "jedis error occurred"}, when the entry is sent through the pipeline and processed, then
     * the response is failed and the message is {@code "jedis error occurred"}.</p>
     */
    @Test
    public void shouldReportFailedForJedisExceptionForStandalone() {
        Response r = Mockito.mock(Response.class);
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        when(r.get()).thenThrow(new JedisException("jedis error occurred"));
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new NoRedisTtl());
        standaloneResponse.process();
        Assert.assertTrue(standaloneResponse.isFailed());
        Assert.assertEquals("jedis error occurred", standaloneResponse.getMessage());
    }

    /**
     * Verifies that a {@link JedisException} from the pipelined TTL call is reported as a failure.
     *
     * <p>Given the pipelined {@code SET} resolving to {@code "OK"} but the deferred {@code EXPIRE}
     * throwing a {@link JedisException} for a {@link DurationTtl} of {@code 1000}, when the entry is
     * sent through the pipeline and processed, then the response is failed and the message is
     * {@code "jedis error occurred"}.</p>
     */
    @Test
    public void shouldReportFailedForJedisExceptionFromTTLForStandalone() {
        Response r = Mockito.mock(Response.class);
        when(r.get()).thenReturn("OK");
        Response tr = Mockito.mock(Response.class);
        when(tr.get()).thenThrow(new JedisException("jedis error occurred"));
        when(pipeline.set("test-key", "test-value")).thenReturn(r);
        when(pipeline.expire("test-key", 1000)).thenReturn(tr);
        RedisStandaloneResponse standaloneResponse = redisKeyValueEntry.send(pipeline, new DurationTtl(1000));
        standaloneResponse.process();
        Assert.assertTrue(standaloneResponse.isFailed());
        Assert.assertEquals("jedis error occurred", standaloneResponse.getMessage());
    }
}

