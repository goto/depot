package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.RedisSinkMetrics;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;


@RunWith(MockitoJUnitRunner.class)
public class RedisStandaloneClientTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private RedisTtl redisTTL;
    @Mock
    private Jedis jedis;

    @Mock
    private DefaultJedisClientConfig defaultJedisClientConfig;
    @Mock
    private HostAndPort hostAndPort;

    @Mock
    private RedisSinkMetrics redisSinkMetrics;


    @Test
    public void shouldCloseTheClient() throws IOException {
        RedisClient redisClient = new RedisStandaloneClient(instrumentation, redisTTL, defaultJedisClientConfig, hostAndPort, jedis, 0, 2000, redisSinkMetrics);
        redisClient.close();

        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Closing Jedis client");
        Mockito.verify(jedis, Mockito.times(1)).close();
    }

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
        Mockito.verify(pipeline, Mockito.times(1)).multi();
        Mockito.verify(pipeline, Mockito.times(1)).sync();
        Mockito.verify(instrumentation, Mockito.times(1)).logDebug("jedis responses: {}", ob);
        IntStream.range(0, actualResponses.size()).forEach(
                index -> {
                    Assert.assertEquals(responses.get(index), actualResponses.get(index));
                }
        );
    }
}
