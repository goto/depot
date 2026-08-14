package com.gotocompany.depot.redis.client.response;

/**
 * Outcome of writing a single record to Redis, independent of deployment topology.
 *
 * <p>Both the standalone and cluster clients translate the result of each command into a
 * {@code RedisResponse} so that the rest of the sink can uniformly decide whether a record failed and
 * obtain a human-readable description of what happened. The implementations are
 * {@link RedisStandaloneResponse} and {@link RedisClusterResponse}.</p>
 *
 * @see RedisStandaloneResponse
 * @see RedisClusterResponse
 */
public interface RedisResponse {
    /**
     * Returns a human-readable description of the command outcome.
     *
     * <p>For a successful write this typically combines the executed command, its result and the TTL
     * status; for a failure it carries the error message reported by Redis.</p>
     *
     * @return the message describing the result, or the error detail when the command failed
     */
    String getMessage();

    /**
     * Indicates whether the command represented by this response failed.
     *
     * @return {@code true} if the command failed, {@code false} if it completed successfully
     */
    boolean isFailed();
}
