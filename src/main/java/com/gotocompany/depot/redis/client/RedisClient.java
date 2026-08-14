package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.record.RedisRecord;

import java.io.Closeable;
import java.util.List;

/**
 * Redis client interface to be used in RedisSink.
 *
 * <p>Implementations encapsulate a specific Redis deployment topology and hide the differences behind
 * a uniform contract: the standalone implementation pipelines all records of a batch inside a single
 * transaction, whereas the cluster implementation issues the commands individually. Because the
 * backing Jedis client is not thread safe, a {@code RedisClient} instance is intended to be owned and
 * used by a single thread.</p>
 *
 * <p>As a {@link Closeable}, the client also exposes {@code close()} to release its underlying
 * connection once the owning sink is shut down.</p>
 *
 * @see RedisStandaloneClient
 * @see RedisClusterClient
 * @see RedisClientFactory
 */
public interface RedisClient extends Closeable {
    /**
     * Sends a batch of records to Redis and returns a per-record outcome.
     *
     * <p>The returned list is positionally aligned with {@code records}: the response at a given
     * index describes the result of writing the record at the same index. Each implementation decides
     * whether the batch is executed transactionally and whether transient failures are retried.</p>
     *
     * @param records the records to write
     * @return a list of {@link RedisResponse} outcomes, one per record, in the same order as the
     *     input
     */
    List<RedisResponse> send(List<RedisRecord> records);

    /**
     * Initialises (or re-initialises) the underlying Redis connection.
     *
     * <p>This is invoked once before the first {@link #send(List)} and may be called again by an
     * implementation to recreate a broken connection between retries. Implementations that hold no
     * recreatable per-connection state may treat it as a no-op.</p>
     */
    void init();
}
