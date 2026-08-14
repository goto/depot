package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import com.gotocompany.depot.exception.ConfigurationException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RedisClientFactory}, which builds a {@link RedisClient} from the deployment
 * type and Redis URLs in the {@link RedisSinkConfig}.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked {@link RedisSinkConfig} and use an
 * {@link ExpectedException} rule for the failure cases. They verify that a cluster deployment yields a
 * {@link RedisClusterClient} (tolerating surrounding whitespace in the URLs) and that malformed
 * cluster or standalone URLs raise a {@link ConfigurationException}.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisClientFactoryTest {

    /**
     * JUnit rule used to assert the type and message of the expected {@link ConfigurationException}.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Mocked sink configuration whose deployment type and URLs are stubbed per scenario.
     */
    @Mock
    private RedisSinkConfig redisSinkConfig;

    /**
     * Mocked StatsD reporter passed to the factory.
     */
    @Mock
    private StatsDReporter statsDReporter;

    /**
     * Verifies that a cluster deployment with valid URLs yields a cluster client.
     *
     * <p>Given a {@link RedisSinkDeploymentType#CLUSTER} configuration with URLs
     * {@code "0.0.0.0:0, 1.1.1.1:1"}, when {@link RedisClientFactory#getClient} is called, then the
     * returned client is a {@link RedisClusterClient}.</p>
     */
    @Test
    public void shouldGetClusterClient() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.CLUSTER);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("0.0.0.0:0, 1.1.1.1:1");

        RedisClient client = RedisClientFactory.getClient(redisSinkConfig, statsDReporter);

        Assert.assertEquals(RedisClusterClient.class, client.getClass());
    }

    /**
     * Verifies that surrounding whitespace in cluster URLs is tolerated.
     *
     * <p>Given a {@link RedisSinkDeploymentType#CLUSTER} configuration with the padded URLs
     * {@code " 0.0.0.0:0, 1.1.1.1:1 "}, when {@link RedisClientFactory#getClient} is called, then the
     * returned client is still a {@link RedisClusterClient}.</p>
     */
    @Test
    public void shouldGetClusterClientWhenURLHasSpaces() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.CLUSTER);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn(" 0.0.0.0:0, 1.1.1.1:1 ");

        RedisClient client = RedisClientFactory.getClient(redisSinkConfig, statsDReporter);

        Assert.assertEquals(RedisClusterClient.class, client.getClass());
    }

    /**
     * Verifies that a malformed cluster URL is rejected.
     *
     * <p>Given a {@link RedisSinkDeploymentType#CLUSTER} configuration whose last URL is missing a
     * port ({@code "localhost:6379,localhost:6378,localhost"}), when
     * {@link RedisClientFactory#getClient} is called, then a {@link ConfigurationException} with the
     * message {@code "Invalid url(s) for redis cluster: localhost:6379,localhost:6378,localhost"} is
     * thrown.</p>
     */
    @Test
    public void shouldThrowExceptionWhenUrlIsInvalidForCluster() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Invalid url(s) for redis cluster: localhost:6379,localhost:6378,localhost");

        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.CLUSTER);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("localhost:6379,localhost:6378,localhost");

        RedisClient client = RedisClientFactory.getClient(redisSinkConfig, statsDReporter);
    }

    /**
     * Verifies that a malformed standalone URL is rejected.
     *
     * <p>Given a {@link RedisSinkDeploymentType#STANDALONE} configuration whose URL is missing a port
     * ({@code "localhost"}), when {@link RedisClientFactory#getClient} is called, then a
     * {@link ConfigurationException} with the message
     * {@code "Invalid url for redis standalone: localhost"} is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionWhenUrlIsInvalidForStandalone() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Invalid url for redis standalone: localhost");

        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.STANDALONE);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("localhost");

        RedisClientFactory.getClient(redisSinkConfig, statsDReporter);
    }
}
