package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Unit tests for {@link RedisClusterClient}, the {@link RedisClient} that writes records directly to a
 * Redis cluster connection.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked {@link Instrumentation},
 * {@link RedisTtl} and {@link JedisCluster}. They verify that a batch of records is sent and their
 * {@link RedisClusterResponse}s are returned in order, and that {@code close} logs and closes the
 * cluster connection.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisClusterClientTest {
    /**
     * Mocked instrumentation used to verify the close logging.
     */
    @Mock
    private Instrumentation instrumentation;
    /**
     * Mocked TTL strategy passed through to each record's send.
     */
    @Mock
    private RedisTtl redisTTL;
    /**
     * Mocked cluster connection backing the client.
     */
    @Mock
    private JedisCluster jedisCluster;

    /**
     * Verifies that a batch is sent to the cluster and its responses are returned in order.
     *
     * <p>Given six mocked {@link RedisRecord}s each stubbed to produce a {@link RedisClusterResponse}
     * on the cluster, when {@link RedisClient#send} is called, then the returned list matches the
     * per-record responses in order.</p>
     */
    @Test
    public void shouldSendToRedisCluster() {
        RedisClient redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        List<RedisRecord> redisRecords = new ArrayList<RedisRecord>() {{
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
            add(Mockito.mock(RedisRecord.class));
        }};
        List<RedisClusterResponse> responses = new ArrayList<RedisClusterResponse>() {{
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
            add(Mockito.mock(RedisClusterResponse.class));
        }};
        IntStream.range(0, redisRecords.size()).forEach(
                index -> Mockito.when(redisRecords.get(index).send(jedisCluster, redisTTL)).thenReturn(responses.get(index))
        );
        List<RedisResponse> actualResponse = redisClient.send(redisRecords);
        IntStream.range(0, redisRecords.size()).forEach(
                index -> Assert.assertEquals(responses.get(index), actualResponse.get(index)));
    }

    /**
     * Verifies that closing the client logs and closes the cluster connection.
     *
     * <p>Given a {@link RedisClusterClient} wrapping the mocked {@link JedisCluster}, when
     * {@link RedisClient#close()} is called, then it logs {@code "Closing Jedis client"} once and
     * closes the cluster connection once.</p>
     *
     * @throws IOException if closing the client fails
     */
    @Test
    public void shouldCallClose() throws IOException {
        RedisClient redisClient = new RedisClusterClient(instrumentation, redisTTL, jedisCluster);
        redisClient.close();
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Closing Jedis client");
        Mockito.verify(jedisCluster, Mockito.times(1)).close();
    }
}
