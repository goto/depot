package com.gotocompany.depot.redis.util;

import com.google.common.collect.ImmutableMap;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.redis.client.entry.RedisListEntry;
import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.record.RedisRecord;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.DefaultJedisClientConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link RedisSinkUtils}, the static helper that maps per-record Redis responses to
 * message-level errors and builds the Jedis client configuration from the sink configuration.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} and back a real {@link Instrumentation} with a
 * mocked {@link StatsDReporter}. They exercise {@link RedisSinkUtils#getErrorsFromResponse} with
 * position-aligned records and {@link RedisClusterResponse}s, and verify
 * {@link RedisSinkUtils#getJedisConfig} against configurations created via {@link ConfigFactory}.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisSinkUtilsTest {
    /**
     * Mocked StatsD reporter used to construct the real {@link Instrumentation} passed to the helper.
     */
    @Mock
    private StatsDReporter statsDReporter;

    /**
     * Verifies that only failed responses become per-message errors keyed by the record index.
     *
     * <p>Given five valid records at indices {@code 1, 4, 7, 10, 15} aligned positionally with five
     * {@link RedisClusterResponse}s where positions one through three are failures
     * ({@code "FAILED AT 4"}, {@code "FAILED AT 7"}, {@code "FAILED AT 10"}) and the first and last
     * succeed, when {@link RedisSinkUtils#getErrorsFromResponse} is invoked, then the returned map
     * holds exactly three entries keyed by the failing records' indices ({@code 4, 7, 10}), each
     * exposing the response text as the wrapped exception message and an
     * {@link ErrorType#DEFAULT_ERROR} type.</p>
     */
    @Test
    public void shouldGetErrorsFromResponse() {
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 7L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 10L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 15L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(new RedisClusterResponse("LPUSH", "OK", null));
        responses.add(new RedisClusterResponse("FAILED AT 4"));
        responses.add(new RedisClusterResponse("FAILED AT 7"));
        responses.add(new RedisClusterResponse("FAILED AT 10"));
        responses.add(new RedisClusterResponse("LPUSH", "OK", null));
        Map<Long, ErrorInfo> errors = RedisSinkUtils.getErrorsFromResponse(records, responses, new Instrumentation(statsDReporter, RedisSinkUtils.class));
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals("FAILED AT 4", errors.get(4L).getException().getMessage());
        Assert.assertEquals("FAILED AT 7", errors.get(7L).getException().getMessage());
        Assert.assertEquals("FAILED AT 10", errors.get(10L).getException().getMessage());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errors.get(4L).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, errors.get(10L).getErrorType());
    }

    /**
     * Verifies that a batch whose responses all succeed yields no errors.
     *
     * <p>Given records aligned with mocked {@link RedisResponse}s whose
     * {@link RedisResponse#isFailed()} all return {@code false}, when
     * {@link RedisSinkUtils#getErrorsFromResponse} is invoked, then the returned error map is
     * empty.</p>
     */
    @Test
    public void shouldGetEmptyMapWhenNoErrors() {
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 7L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 10L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 15L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.forEach(response -> {
            Mockito.when(response.isFailed()).thenReturn(false);
        });
        Map<Long, ErrorInfo> errors = RedisSinkUtils.getErrorsFromResponse(records, responses, new Instrumentation(statsDReporter, RedisSinkUtils.class));
        Assert.assertTrue(errors.isEmpty());
    }


    /**
     * Verifies that the configured connection timeout is carried into the Jedis client config.
     *
     * <p>Given a {@link RedisSinkConfig} created with {@code SINK_REDIS_CONNECTION_TIMEOUT_MS} set to
     * {@code "5000"}, when {@link RedisSinkUtils#getJedisConfig} builds the
     * {@link DefaultJedisClientConfig}, then its connection timeout equals {@code 5000}
     * milliseconds.</p>
     */
    @Test
    public void shouldSetRedisConnectionTimeoutMillis() {

        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_CONNECTION_TIMEOUT_MS", "5000"
        ));
        DefaultJedisClientConfig defaultJedisClientConfig = RedisSinkUtils.getJedisConfig(config);
        Assert.assertEquals(5000, defaultJedisClientConfig.getConnectionTimeoutMillis());

    }

    /**
     * Verifies that the configured socket timeout is carried into the Jedis client config.
     *
     * <p>Given a {@link RedisSinkConfig} created with {@code SINK_REDIS_SOCKET_TIMEOUT_MS} set to
     * {@code "7000"}, when {@link RedisSinkUtils#getJedisConfig} builds the
     * {@link DefaultJedisClientConfig}, then its socket timeout equals {@code 7000} milliseconds.</p>
     */
    @Test
    public void shouldSetRedisSocketTimeoutMillis() {

        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_SOCKET_TIMEOUT_MS", "7000"
        ));
        DefaultJedisClientConfig defaultJedisClientConfig = RedisSinkUtils.getJedisConfig(config);
        Assert.assertEquals(7000, defaultJedisClientConfig.getSocketTimeoutMillis());

    }

}
