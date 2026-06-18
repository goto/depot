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
 * Unit tests for {@link ExactTimeTtl}, the {@link RedisTtl} strategy that expires keys at an absolute
 * Unix timestamp via the Redis {@code EXPIREAT} command.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked standalone {@link Pipeline} and a
 * mocked {@link JedisCluster}, and verify that the configured timestamp is forwarded on both the
 * pipeline and cluster execution paths.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class ExactTimeTTLTest {

    /**
     * Instance under test, created in {@link #setup()} with a fixed absolute expiry timestamp.
     */
    private ExactTimeTtl exactTimeTTL;
    /**
     * Mocked standalone pipeline used to capture the queued {@code EXPIREAT} command.
     */
    @Mock
    private Pipeline pipeline;

    /**
     * Mocked cluster connection used to capture the applied {@code EXPIREAT} command.
     */
    @Mock
    private JedisCluster jedisCluster;

    /**
     * Creates the {@link ExactTimeTtl} under test configured to expire keys at the absolute Unix
     * timestamp {@code 10000000}.
     */
    @Before
    public void setup() {
        exactTimeTTL = new ExactTimeTtl(10000000L);
    }

    /**
     * Verifies that the absolute expiry timestamp is applied through a standalone pipeline.
     *
     * <p>Given the {@link ExactTimeTtl} configured with timestamp {@code 10000000}, when
     * {@link ExactTimeTtl#setTtl(Pipeline, String)} is invoked for {@code "test-key"}, then
     * {@code Pipeline.expireAt} is called exactly once with that key and timestamp.</p>
     */
    @Test
    public void shouldSetUnixTimeStampAsTTLForPipeline() {
        exactTimeTTL.setTtl(pipeline, "test-key");
        verify(pipeline, times(1)).expireAt("test-key", 10000000L);
    }

    /**
     * Verifies that the absolute expiry timestamp is applied through a cluster connection.
     *
     * <p>Given the {@link ExactTimeTtl} configured with timestamp {@code 10000000}, when
     * {@link ExactTimeTtl#setTtl(JedisCluster, String)} is invoked for {@code "test-key"}, then
     * {@code JedisCluster.expireAt} is called exactly once with that key and timestamp.</p>
     */
    @Test
    public void shouldSetUnixTimeStampAsTTLForCluster() {
        exactTimeTTL.setTtl(jedisCluster, "test-key");
        verify(jedisCluster, times(1)).expireAt("test-key", 10000000L);
    }
}
