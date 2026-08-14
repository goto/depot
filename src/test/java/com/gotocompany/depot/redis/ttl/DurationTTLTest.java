package com.gotocompany.depot.redis.ttl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link DurationTtl}, the {@link RedisTtl} strategy that expires keys after a relative
 * duration via the Redis {@code EXPIRE} command.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked standalone {@link Pipeline} and a
 * mocked {@link JedisCluster}, and verify that the configured duration in seconds is forwarded on both
 * the pipeline and cluster execution paths.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class DurationTTLTest {

    /**
     * Instance under test, created in {@link #setup()} with a fixed relative duration.
     */
    private DurationTtl durationTTL;

    /**
     * Mocked standalone pipeline used to capture the queued {@code EXPIRE} command.
     */
    @Mock
    private Pipeline pipeline;

    /**
     * Mocked cluster connection used to capture the applied {@code EXPIRE} command.
     */
    @Mock
    private JedisCluster jedisCluster;

    /**
     * Creates the {@link DurationTtl} under test configured to expire keys {@code 10} seconds after
     * they are written.
     */
    @Before
    public void setup() {
        durationTTL = new DurationTtl(10);
    }

    /**
     * Verifies that the relative duration is applied through a standalone pipeline.
     *
     * <p>Given the {@link DurationTtl} configured with {@code 10} seconds, when
     * {@link DurationTtl#setTtl(Pipeline, String)} is invoked for {@code "test-key"}, then
     * {@code Pipeline.expire} is called exactly once with that key and duration.</p>
     */
    @Test
    public void shouldSetTTLInSecondsForPipeline() {
        durationTTL.setTtl(pipeline, "test-key");
        verify(pipeline, times(1)).expire("test-key", 10);
    }

    /**
     * Verifies that the relative duration is applied through a cluster connection.
     *
     * <p>Given the {@link DurationTtl} configured with {@code 10} seconds, when
     * {@link DurationTtl#setTtl(JedisCluster, String)} is invoked for {@code "test-key"}, then
     * {@code JedisCluster.expire} is called exactly once with that key and duration.</p>
     */
    @Test
    public void shouldSetTTLInSecondsForCluster() {
        durationTTL.setTtl(jedisCluster, "test-key");
        verify(jedisCluster, times(1)).expire("test-key", 10);
    }
}
