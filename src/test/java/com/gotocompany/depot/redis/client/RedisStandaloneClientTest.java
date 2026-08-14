package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.RedisSinkMetrics;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link RedisStandaloneClient}, the {@link RedisClient} that writes records to a
 * standalone Redis through a pipelined transaction and instruments the outcome.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked {@link Instrumentation},
 * {@link RedisTtl} and Jedis {@link Jedis} connection, plus a real {@link RedisSinkMetrics} built from
 * a {@link ConfigFactory} configuration that carries the {@code xyz_} metrics prefix. They verify the
 * pipelined send flow (multi/sync, debug logging and the returned per-record responses), the success
 * and no-response counters, the connection-retry counter, and that {@code close} shuts down the
 * underlying connection.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisStandaloneClientTest {
    /**
     * Mocked instrumentation used to verify logging and metric capture.
     */
    @Mock
    private Instrumentation instrumentation;
    /**
     * Mocked TTL strategy passed through to each record's send.
     */
    @Mock
    private RedisTtl redisTTL;
    /**
     * Mocked standalone Jedis connection backing the client.
     */
    @Mock
    private Jedis jedis;

    /**
     * Mocked Jedis client configuration supplied to the client constructor.
     */
    @Mock
    private DefaultJedisClientConfig defaultJedisClientConfig;
    /**
     * Mocked Redis host and port supplied to the client constructor.
     */
    @Mock
    private HostAndPort hostAndPort;


    /**
     * Real metrics holder built in {@link #setUp()} with the {@code xyz_} application prefix.
     */
    private RedisSinkMetrics redisSinkMetrics;

    /**
     * Sets the {@code SINK_METRICS_APPLICATION_PREFIX} system property to {@code "xyz_"} and builds the
     * {@link RedisSinkMetrics} from a {@link ConfigFactory} configuration so the asserted metric names
     * carry that prefix.
     */
    @Before
    public void setUp() {
        System.setProperty("SINK_METRICS_APPLICATION_PREFIX", "xyz_");
        RedisSinkConfig sinkConfig = ConfigFactory.create(RedisSinkConfig.class, System.getProperties());
        redisSinkMetrics = new RedisSinkMetrics(sinkConfig);
    }

    /**
     * Verifies that closing the client logs and closes the underlying connection.
     *
     * <p>Given a {@link RedisStandaloneClient} wrapping the mocked {@link Jedis}, when
     * {@link RedisClient#close()} is called, then it logs {@code "Closing Jedis client"} once and
     * closes the Jedis connection once.</p>
     *
     * @throws IOException if closing the client fails
     */
    @Test
    public void shouldCloseTheClient() throws IOException {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 0, 2000, redisSinkMetrics);
        redisClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedis, times(1)).close();
    }

    /**
     * Verifies the pipelined send flow and the responses returned for a batch.
     *
     * <p>Given six mocked {@link RedisRecord}s each stubbed to produce a processed
     * {@link RedisStandaloneResponse} on the pipeline, when {@link RedisClient#send} is called, then
     * the transaction is opened and flushed exactly once ({@code multi}/{@code sync}), the raw Jedis
     * result is logged at debug level, and the returned list matches the per-record responses in
     * order.</p>
     */
    @Test
    public void shouldSendRecordsToJedis() {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 0, 2000, redisSinkMetrics);
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Response response = Mockito.mock(Response.class);
        Mockito.when(jedis.pipelined()).thenReturn(pipeline);
        Mockito.when(pipeline.exec()).thenReturn(response);
        Object ob = new Object();
        Mockito.when(response.get()).thenReturn(ob);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};
        List<RedisStandaloneResponse> responses = new ArrayList<RedisStandaloneResponse>() {{
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
        }};
        IntStream.range(0, redisRecords.size()).forEach(
                index -> {
                    Mockito.when(redisRecords.get(index).send(pipeline, redisTTL)).thenReturn(responses.get(index));
                    Mockito.when(responses.get(index).process()).thenReturn(responses.get(index));
                }
        );
        List<RedisResponse> actualResponses = redisClient.send(redisRecords);
        verify(pipeline, times(1)).multi();
        verify(pipeline, times(1)).sync();
        verify(instrumentation, times(1)).logDebug("jedis responses: {}", ob);
        IntStream.range(0, actualResponses.size()).forEach(
                index -> {
                    Assert.assertEquals(responses.get(index), actualResponses.get(index));
                }
        );
    }

    /**
     * Verifies that successful writes increment the success counter.
     *
     * <p>Given six records whose pipelined responses all succeed, when {@link RedisClient#send} is
     * called, then the metric {@code "xyz_sink_redis_success_response_total"} is captured once with the
     * value {@code 6}.</p>
     */
    @Test
    public void shouldInstrumentSuccess() {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 0, 2000, redisSinkMetrics);
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Response response = Mockito.mock(Response.class);
        Mockito.when(jedis.pipelined()).thenReturn(pipeline);
        Mockito.when(pipeline.exec()).thenReturn(response);
        Object ob = new Object();
        Mockito.when(response.get()).thenReturn(ob);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};
        List<RedisStandaloneResponse> responses = new ArrayList<RedisStandaloneResponse>() {{
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
            add(Mockito.mock(RedisStandaloneResponse.class));
        }};
        IntStream.range(0, redisRecords.size()).forEach(
                index -> {
                    Mockito.when(redisRecords.get(index).send(pipeline, redisTTL)).thenReturn(responses.get(index));
                    Mockito.when(responses.get(index).process()).thenReturn(responses.get(index));
                }
        );
        redisClient.send(redisRecords);
        verify(instrumentation, times(1)).captureCount("xyz_sink_redis_success_response_total", 6L);

    }

    /**
     * Verifies that a connection failure during send increments the no-response counter.
     *
     * <p>Given six records whose pipelined send each throws a {@link JedisConnectionException}, when
     * {@link RedisClient#send} is called (and the propagated exception is swallowed by the test), then
     * the metric {@code "xyz_sink_redis_no_response_total"} is captured once with the value
     * {@code 6}.</p>
     */
    @Test
    public void shouldInstrumentFailure() {

        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 0, 2000, redisSinkMetrics);
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Mockito.when(jedis.pipelined()).thenReturn(pipeline);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};

        IntStream.range(0, redisRecords.size()).forEach(
                index -> Mockito.when(redisRecords.get(index).send(pipeline, redisTTL)).thenThrow(JedisConnectionException.class)
        );
        try {
            redisClient.send(redisRecords);
        } catch (JedisConnectionException ignored) {
        }
        verify(instrumentation, times(1)).captureCount("xyz_sink_redis_no_response_total", 6L);

    }

    /**
     * Verifies that a configured connection retry is instrumented.
     *
     * <p>Given a {@link RedisStandaloneClient} configured with one connection retry, when
     * {@link RedisClient#send} is called and fails (the error is swallowed by the test), then the
     * metric {@code "xyz_sink_redis_connection_retry_total"} is captured once with the value
     * {@code 1}.</p>
     */
    @Test
    public void shouldInstrumentConnectionRetry() {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 1, 2000, redisSinkMetrics);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};

        try {
            redisClient.send(redisRecords);
        } catch (Exception ignored) {
        }
        verify(instrumentation, times(1)).captureCount("xyz_sink_redis_connection_retry_total", 1L);


    }

}
