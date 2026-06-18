package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import com.gotocompany.depot.metrics.Instrumentation;
import lombok.AllArgsConstructor;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Redis cluster client.
 *
 * <p>Unlike the standalone client, a cluster spreads keys across many nodes, so a single transaction
 * cannot span the whole batch. This client therefore sends each record individually through
 * {@link RedisRecord#send(JedisCluster, RedisTtl)} and collects the per-record {@link RedisResponse}
 * results. The shared {@link JedisCluster}, created and configured by {@link RedisClientFactory},
 * manages connection pooling and command routing internally.</p>
 *
 * @see RedisStandaloneClient
 * @see RedisClientFactory
 */
@AllArgsConstructor
public class RedisClusterClient implements RedisClient {

    /**
     * Logging facade used to report the client lifecycle (currently on close).
     */
    private final Instrumentation instrumentation;
    /**
     * Strategy that applies the configured time-to-live to each written key.
     */
    private final RedisTtl redisTTL;
    /**
     * Shared cluster connection used to route and execute every command.
     */
    private final JedisCluster jedisCluster;

    /**
     * Sends each record to the cluster individually and collects the per-record outcomes.
     *
     * <p>Because cluster keys may live on different nodes, the batch is not executed as a single
     * transaction; instead every record is written via
     * {@link RedisRecord#send(JedisCluster, RedisTtl)}, which also applies the TTL strategy and
     * converts any thrown {@code JedisException} into a failed
     * {@link com.gotocompany.depot.redis.client.response.RedisClusterResponse}.</p>
     *
     * @param records the records to write
     * @return a list of {@link RedisResponse} outcomes, one per record, in input order
     */
    @Override
    public List<RedisResponse> send(List<RedisRecord> records) {
        return records.stream()
                .map(record -> record.send(jedisCluster, redisTTL))
                .collect(Collectors.toList());
    }

    /**
     * Performs no initialisation.
     *
     * <p>The {@link JedisCluster} is fully constructed and ready when this client is created, so there
     * is no connection to (re)establish here.</p>
     */
    @Override
    public void init() {
    }

    /**
     * Closes the underlying {@link JedisCluster}.
     *
     * <p>Logs the closure and releases the cluster's pooled connections.</p>
     */
    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedisCluster.close();
    }
}
