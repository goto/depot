package com.gotocompany.depot.redis.client.response;

import lombok.Getter;

/**
 * {@link RedisResponse} for the cluster client.
 *
 * <p>Cluster commands are executed eagerly rather than pipelined, so the outcome is known immediately.
 * The success constructor builds a descriptive message from the command, its result and the TTL
 * status and marks the response as not failed, while the failure constructor wraps an error message
 * and marks the response as failed.</p>
 *
 * @see com.gotocompany.depot.redis.client.RedisClusterClient
 */
public class RedisClusterResponse implements RedisResponse {
    /**
     * Human-readable description of the outcome, or the error detail on failure.
     */
    @Getter
    private final String message;
    /**
     * Failure flag; {@code false} for the success constructor and {@code true} for the failure
     * constructor.
     */
    @Getter
    private final boolean failed;

    /**
     * Creates a successful response describing an executed cluster command.
     *
     * <p>The message is rendered as {@code command: response, TTL: status}, where the TTL status is
     * {@code NoOp} when {@code ttlResponse} is {@code null}, {@code NOT UPDATED} when it is {@code 0}
     * and {@code UPDATED} otherwise.</p>
     *
     * @param command the name of the executed command, for example {@code SET}
     * @param response the value returned by the command
     * @param ttlResponse the result of the TTL command, or {@code null} when no TTL was applied
     */
    public RedisClusterResponse(String command, Object response, Long ttlResponse) {
        this.message = String.format(
                "%s: %s, TTL: %s",
                command,
                response,
                ttlResponse == null ? "NoOp" : ttlResponse == 0 ? "NOT UPDATED" : "UPDATED");
        this.failed = false;
    }

    /**
     * Creates a failed response carrying an error message.
     *
     * @param message the error message describing why the command failed
     */
    public RedisClusterResponse(String message) {
        this.message = message;
        this.failed = true;
    }
}
