package com.gotocompany.depot.redis.client;


import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import com.gotocompany.depot.redis.ttl.RedisTTLFactory;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import com.gotocompany.depot.redis.util.RedisSinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;

/**
 * Redis client factory.
 *
 * <p>Based on {@link RedisSinkConfig#getSinkRedisDeploymentType()} it returns either a
 * {@link RedisClusterClient} backed by a {@link JedisCluster} spanning the configured nodes, or a
 * {@link RedisStandaloneClient} connected to a single host. The shared {@link RedisTtl} strategy is
 * resolved once via {@link RedisTTLFactory} and passed to the cluster client.</p>
 */
public class RedisClientFactory {

    /**
     * Separator used to split the configured comma-delimited list of Redis cluster node urls.
     */
    private static final String DELIMITER = ",";

    /**
     * Creates the {@link RedisClient} matching the configured deployment type.
     *
     * <p>When {@link RedisSinkConfig#getSinkRedisDeploymentType()} is
     * {@link RedisSinkDeploymentType#CLUSTER} a {@link RedisClusterClient} is built (see
     * {@link #getRedisClusterClient(RedisTtl, RedisSinkConfig, StatsDReporter)}); otherwise a
     * {@link RedisStandaloneClient} is returned. The TTL strategy shared by the client is resolved
     * from the configuration via {@link RedisTTLFactory#getTTl(RedisSinkConfig)}.</p>
     *
     * @param redisSinkConfig the Redis sink configuration
     * @param statsDReporter the reporter used to build the client's {@link Instrumentation}
     * @return a standalone or cluster {@link RedisClient} depending on the configured deployment type
     */
    public static RedisClient getClient(RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkConfig.getSinkRedisDeploymentType();
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        return RedisSinkDeploymentType.CLUSTER.equals(redisSinkDeploymentType)
                ? getRedisClusterClient(redisTTL, redisSinkConfig, statsDReporter)
                : new RedisStandaloneClient(new Instrumentation(statsDReporter, RedisStandaloneClient.class), redisSinkConfig);
    }


    /**
     * Builds a {@link RedisClusterClient} from the configured cluster node urls.
     *
     * <p>The comma-separated {@link RedisSinkConfig#getSinkRedisUrls()} value is split on
     * {@link #DELIMITER}, each entry is trimmed and parsed into a {@link HostAndPort}, and the
     * resulting set of nodes is used to construct a {@link JedisCluster} with the client configuration
     * from {@link RedisSinkUtils#getJedisConfig(RedisSinkConfig)}, the configured maximum number of
     * attempts and a default connection pool. The cluster client shares the supplied {@link RedisTtl}
     * strategy.</p>
     *
     * @param redisTTL the TTL strategy applied to keys written through the cluster client
     * @param redisSinkConfig the Redis sink configuration providing the node urls and connection
     *     settings
     * @param statsDReporter the reporter used to build the client's {@link Instrumentation}
     * @return a configured {@link RedisClusterClient}
     * @throws ConfigurationException if any of the configured urls cannot be parsed into a host and
     *     port
     */
    private static RedisClusterClient getRedisClusterClient(RedisTtl redisTTL, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        String[] redisUrls = redisSinkConfig.getSinkRedisUrls().split(DELIMITER);
        HashSet<HostAndPort> nodes = new HashSet<>();
        try {
            for (String redisUrl : redisUrls) {
                nodes.add(HostAndPort.parseString(StringUtils.trim(redisUrl)));
            }
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(String.format("Invalid url(s) for redis cluster: %s", redisSinkConfig.getSinkRedisUrls()));
        }

        JedisCluster jedisCluster = new JedisCluster(nodes, RedisSinkUtils.getJedisConfig(redisSinkConfig), redisSinkConfig.getSinkRedisMaxAttempts(), new GenericObjectPoolConfig<>());
        return new RedisClusterClient(new Instrumentation(statsDReporter, RedisClusterClient.class), redisTTL, jedisCluster);
    }
}
