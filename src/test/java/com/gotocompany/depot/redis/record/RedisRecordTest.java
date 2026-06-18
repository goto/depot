package com.gotocompany.depot.redis.record;

import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RedisRecord}, the per-message unit of work the Redis sink produces by pairing
 * a {@link RedisEntry} with its index, error info, metadata and validity flag.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked {@link RedisEntry}, {@link RedisTtl}
 * and Jedis {@link JedisCluster}/{@link Pipeline} targets. They verify that the {@code send} overloads
 * delegate to the wrapped entry and that the accessors and {@link RedisRecord#toString()} expose the
 * record's state.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisRecordTest {
    /**
     * Mocked entry whose send and {@code toString} behaviour the record under test delegates to.
     */
    @Mock
    private RedisEntry redisEntry;
    /**
     * Mocked cluster connection passed to the cluster {@code send} overload.
     */
    @Mock
    private JedisCluster jedisCluster;
    /**
     * Mocked standalone pipeline passed to the pipeline {@code send} overload.
     */
    @Mock
    private Pipeline jedisPipeline;
    /**
     * Mocked TTL strategy forwarded unchanged to the wrapped entry.
     */
    @Mock
    private RedisTtl redisTtl;

    /**
     * Verifies that the cluster {@code send} overload delegates to the wrapped entry.
     *
     * <p>Given a mocked {@link RedisEntry} stubbed to return a {@link RedisClusterResponse} for
     * {@code send(jedisCluster, redisTtl)}, when {@link RedisRecord#send(JedisCluster, RedisTtl)} is
     * called, then the record returns the very response the entry produced.</p>
     */
    @Test
    public void shouldSendUsingCLusterClient() {
        RedisClusterResponse response = Mockito.mock(RedisClusterResponse.class);
        when(redisEntry.send(jedisCluster, redisTtl)).thenReturn(response);
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        RedisClusterResponse redisClusterResponse = redisRecord.send(jedisCluster, redisTtl);
        Assert.assertEquals(response, redisClusterResponse);
    }

    /**
     * Verifies that the standalone {@code send} overload delegates to the wrapped entry.
     *
     * <p>Given a mocked {@link RedisEntry} stubbed to return a {@link RedisStandaloneResponse} for
     * {@code send(jedisPipeline, redisTtl)}, when {@link RedisRecord#send(Pipeline, RedisTtl)} is
     * called, then the record returns the very response the entry produced.</p>
     */
    @Test
    public void shouldSendUsingStandaloneClient() {
        RedisStandaloneResponse standaloneResponse = Mockito.mock(RedisStandaloneResponse.class);
        when(redisEntry.send(jedisPipeline, redisTtl)).thenReturn(standaloneResponse);
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        RedisStandaloneResponse redisResponse = redisRecord.send(jedisPipeline, redisTtl);
        Assert.assertEquals(standaloneResponse, redisResponse);
    }

    /**
     * Verifies that {@link RedisRecord#toString()} combines metadata with the wrapped entry's text.
     *
     * <p>Given a record with metadata {@code "METADATA"} wrapping an entry whose {@code toString} is
     * stubbed to {@code "RedisEntry REDIS ENTRY TO STRING"}, when {@link RedisRecord#toString()} is
     * called, then it returns {@code "Metadata METADATA RedisEntry REDIS ENTRY TO STRING"}.</p>
     */
    @Test
    public void shouldGetToString() {
        when(redisEntry.toString()).thenReturn("RedisEntry REDIS ENTRY TO STRING");
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        Assert.assertEquals("Metadata METADATA RedisEntry REDIS ENTRY TO STRING", redisRecord.toString());
    }

    /**
     * Verifies that the record exposes the index it was constructed with.
     *
     * <p>Given a record built with index {@code 0}, when {@link RedisRecord#getIndex()} is called,
     * then it returns {@code 0}.</p>
     */
    @Test
    public void shouldGetRecordIndex() {
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        Assert.assertEquals(new Long(0), redisRecord.getIndex());
    }

    /**
     * Verifies that the record exposes the error info it was constructed with.
     *
     * <p>Given a record built with a specific {@link ErrorInfo} of type
     * {@link ErrorType#DEFAULT_ERROR}, when {@link RedisRecord#getErrorInfo()} is called, then it
     * returns that same instance.</p>
     */
    @Test
    public void shouldGetRecordErrorInfo() {
        ErrorInfo errorInfo = new ErrorInfo(new Exception(""), ErrorType.DEFAULT_ERROR);
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, errorInfo, "METADATA", true);
        Assert.assertEquals(errorInfo, redisRecord.getErrorInfo());
    }

    /**
     * Verifies that the record exposes the validity flag it was constructed with.
     *
     * <p>Given a record built with the valid flag set to {@code true}, when
     * {@link RedisRecord#isValid()} is called, then it returns {@code true}.</p>
     */
    @Test
    public void shouldGetRecordValidBoolean() {
        RedisRecord redisRecord = new RedisRecord(redisEntry, 0L, null, "METADATA", true);
        Assert.assertTrue(redisRecord.isValid());
    }
}
