package com.gotocompany.depot.redis.enums;

/**
 * Redis deployment topology the sink connects to, selected by {@code SINK_REDIS_DEPLOYMENT_TYPE}.
 *
 * <p>It decides which {@link com.gotocompany.depot.redis.client.RedisClient} implementation
 * {@link com.gotocompany.depot.redis.client.RedisClientFactory} creates.</p>
 */
public enum RedisSinkDeploymentType {
    /**
     * A single Redis server addressed by one url; writes use a transactional pipeline.
     */
    STANDALONE,
    /**
     * A Redis Cluster spanning several nodes; commands are routed per key and sent individually.
     */
    CLUSTER
}
