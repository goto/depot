package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

/**
 * Metric-name definitions for Depot's Redis sink.
 *
 * <p>{@code RedisSinkMetrics} extends {@link SinkMetrics} with metric names specific to the Redis
 * sink, covering successful responses, absent responses and connection retries. It is consumed by
 * Redis client components such as {@code RedisStandaloneClient}.
 *
 * <p>Its metric names build on the inherited application prefix and {@code sink_} segment, with an
 * additional {@code redis_} segment supplied by {@link #REDIS_SINK_PREFIX}.
 */
public class RedisSinkMetrics extends SinkMetrics {

    /**
     * Sink-specific metric-name segment ({@code "redis_"}) inserted after the {@code sink_} segment
     * for all Redis sink metric names.
     */
    public static final String REDIS_SINK_PREFIX = "redis_";

    /**
     * Creates a Redis-sink metrics helper bound to the supplied sink configuration.
     *
     * @param config the sink configuration supplying the application metric prefix
     */
    public RedisSinkMetrics(SinkConfig config) {
        super(config);
    }

    /**
     * Returns the metric name counting successful Redis responses.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "redis_" + "success_response_total"}.
     *
     * @return the Redis success-response-total metric name
     */
    public String getRedisSuccessResponseTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + REDIS_SINK_PREFIX + "success_response_total";
    }

    /**
     * Returns the metric name counting Redis calls that produced no response.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "redis_" + "no_response_total"}.
     *
     * @return the Redis no-response-total metric name
     */
    public String getRedisNoResponseTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + REDIS_SINK_PREFIX + "no_response_total";
    }

    /**
     * Returns the metric name counting Redis connection retries.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "redis_" + "connection_retry_total"}.
     *
     * @return the Redis connection-retry-total metric name
     */
    public String getRedisConnectionRetryTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + REDIS_SINK_PREFIX + "connection_retry_total";
    }
}
