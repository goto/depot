package com.gotocompany.depot.config;

import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * Unit tests for {@link RedisSinkConfig}, the Owner-based configuration interface for the Redis
 * sink.
 *
 * <p>Each test builds a {@link RedisSinkConfig} via {@link ConfigFactory} from either system
 * properties or an explicit {@link Properties} map and asserts the values returned by its typed
 * accessors. The cases cover enum-typed deployment and TTL settings, the empty-string-to-{@code null}
 * normalization applied to the optional auth credentials, and the whitespace trimming applied to
 * string settings.
 */
public class RedisSinkConfigTest {
    /**
     * Verifies that {@link RedisSinkConfig} resolves the deployment type, TTL type and key template
     * from configuration.
     *
     * <p>Given {@code SINK_REDIS_DEPLOYMENT_TYPE=standalone}, {@code SINK_REDIS_TTL_TYPE=disable}
     * and {@code SINK_REDIS_KEY_TEMPLATE=test-key}, when the config is built, then
     * {@link RedisSinkConfig#getSinkRedisKeyTemplate()} returns {@code "test-key"},
     * {@link RedisSinkConfig#getSinkRedisDeploymentType()} returns
     * {@link RedisSinkDeploymentType#STANDALONE} and {@link RedisSinkConfig#getSinkRedisTtlType()}
     * returns {@link RedisSinkTtlType#DISABLE}.
     */
    @Test
    public void testMetadataTypes() {
        System.setProperty("SINK_REDIS_DEPLOYMENT_TYPE", "standalone");
        System.setProperty("SINK_REDIS_TTL_TYPE", "disable");
        System.setProperty("SINK_REDIS_KEY_TEMPLATE", "test-key");
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, System.getProperties());
        Assert.assertEquals("test-key", config.getSinkRedisKeyTemplate());
        Assert.assertEquals(RedisSinkDeploymentType.STANDALONE, config.getSinkRedisDeploymentType());
        Assert.assertEquals(RedisSinkTtlType.DISABLE, config.getSinkRedisTtlType());
    }

    /**
     * Verifies that blank Redis auth credentials are normalized to {@code null}.
     *
     * <p>Given {@code SINK_REDIS_AUTH_USERNAME} and {@code SINK_REDIS_AUTH_PASSWORD} both set to
     * empty strings, when the config is built, then
     * {@link RedisSinkConfig#getSinkRedisAuthUsername()} and
     * {@link RedisSinkConfig#getSinkRedisAuthPassword()} both return {@code null}.
     */
    @Test
    public void shouldSetNullIfAuthConfigsSetAsEmptyString() {
        Properties properties = new Properties();
        properties.setProperty("SINK_REDIS_AUTH_USERNAME", "");
        properties.setProperty("SINK_REDIS_AUTH_PASSWORD", "");
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, properties);
        Assert.assertNull(config.getSinkRedisAuthUsername());
        Assert.assertNull(config.getSinkRedisAuthPassword());
    }
    /**
     * Verifies that absent Redis auth credentials resolve to {@code null}.
     *
     * <p>Given an empty property map, when the config is built, then
     * {@link RedisSinkConfig#getSinkRedisAuthUsername()} and
     * {@link RedisSinkConfig#getSinkRedisAuthPassword()} both return {@code null}.
     */
    @Test
    public void shouldReturnNullIfAuthConfigsNotSet() {
        Properties properties = new Properties();
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, properties);
        Assert.assertNull(config.getSinkRedisAuthUsername());
        Assert.assertNull(config.getSinkRedisAuthPassword());
    }

    /**
     * Verifies that populated Redis auth credentials are returned unchanged.
     *
     * <p>Given {@code SINK_REDIS_AUTH_USERNAME=user} and {@code SINK_REDIS_AUTH_PASSWORD=pwd}, when
     * the config is built, then {@link RedisSinkConfig#getSinkRedisAuthUsername()} returns
     * {@code "user"} and {@link RedisSinkConfig#getSinkRedisAuthPassword()} returns {@code "pwd"}.
     */
    @Test
    public void shouldReturnConfigsIfAuthConfigsNotEmpty() {
        Properties properties = new Properties();
        properties.setProperty("SINK_REDIS_AUTH_USERNAME", "user");
        properties.setProperty("SINK_REDIS_AUTH_PASSWORD", "pwd");
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, properties);
        Assert.assertEquals("user", config.getSinkRedisAuthUsername());
        Assert.assertEquals("pwd", config.getSinkRedisAuthPassword());
    }

    /**
     * Verifies that surrounding whitespace is trimmed from Redis string settings.
     *
     * <p>Given {@code SINK_REDIS_URLS} set to {@code "     0.0.0.0:8000      "} and
     * {@code SINK_REDIS_AUTH_USERNAME} set to {@code " user "}, when the config is built, then
     * {@link RedisSinkConfig#getSinkRedisUrls()} returns {@code "0.0.0.0:8000"},
     * {@link RedisSinkConfig#getSinkRedisAuthUsername()} returns {@code "user"}, and the unset
     * {@link RedisSinkConfig#getSinkRedisAuthPassword()} returns {@code null}.
     */
    @Test
    public void shouldRemoveWhiteSpacesFromConfigs() {
        Properties properties = new Properties();
        properties.setProperty("SINK_REDIS_URLS", "     0.0.0.0:8000      ");
        properties.setProperty("SINK_REDIS_AUTH_USERNAME", " user ");
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, properties);
        Assert.assertEquals("0.0.0.0:8000", config.getSinkRedisUrls());
        Assert.assertEquals("user", config.getSinkRedisAuthUsername());
        Assert.assertNull(config.getSinkRedisAuthPassword());
    }
}
