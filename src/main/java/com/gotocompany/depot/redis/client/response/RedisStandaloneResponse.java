package com.gotocompany.depot.redis.client.response;

import lombok.Getter;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

/**
 * {@link RedisResponse} for the standalone (pipelined, transactional) client.
 *
 * <p>Standalone writes are queued on a Jedis pipeline, so when this object is first created the actual
 * command result is not yet known: it holds the deferred {@link Response} handles for the write
 * command and the optional TTL command. After the transaction is executed and synchronised,
 * {@link #process()} resolves those handles into a final {@link #getMessage() message} and
 * {@link #isFailed() failed} flag. The response is considered failed until it has been processed
 * successfully.</p>
 *
 * @see com.gotocompany.depot.redis.client.RedisStandaloneClient
 */
public class RedisStandaloneResponse implements RedisResponse {
    /**
     * Deferred handle for the data command (for example {@code SET}, {@code LPUSH} or {@code HSET})
     * queued on the pipeline.
     */
    private final Response response;
    /**
     * Deferred handle for the optional TTL command, or {@code null} when no TTL is applied.
     */
    private final Response ttlResponse;
    /**
     * Name of the executed command, used when building the result message.
     */
    private final String command;
    /**
     * Human-readable description of the outcome, populated by {@link #process()}.
     */
    @Getter
    private String message;
    /**
     * Failure flag; {@code true} until {@link #process()} resolves the command successfully.
     */
    @Getter
    private boolean failed = true;

    /**
     * Creates an unresolved response capturing the deferred pipeline handles for a single record.
     *
     * @param command the name of the data command that was queued, for example {@code SET}
     * @param response the deferred {@link Response} for the data command
     * @param ttlResponse the deferred {@link Response} for the TTL command, or {@code null} if no TTL
     *     is applied
     */
    public RedisStandaloneResponse(String command, Response response, Response ttlResponse) {
        this.command = command;
        this.response = response;
        this.ttlResponse = ttlResponse;
    }

    /**
     * Resolves the deferred pipeline handles into a final outcome.
     *
     * <p>This must be called only after the surrounding transaction has been executed and the pipeline
     * synchronised. It reads the data command result and, when present, the TTL command result,
     * rendering a message of the form {@code command: result, TTL: status} where the TTL status is
     * {@code UPDATED}, {@code NOT UPDATED} (when the TTL result is {@code 0}) or {@code NoOp} (when no
     * TTL command was queued), and marks the response as not failed. If
     * resolving the result throws a {@link JedisException}, the response is instead marked as failed
     * and its message is set to the exception's message.</p>
     *
     * @return this response, after its {@link #getMessage() message} and {@link #isFailed() failed}
     *     flag have been populated
     */
    public RedisStandaloneResponse process() {
        try {
            Object cmd = response.get();
            Object ttl = ttlResponse != null ? (((long) ttlResponse.get()) == 0L ? "NOT UPDATED" : "UPDATED") : "NoOp";
            message = String.format("%s: %s, TTL: %s", command, cmd, ttl);
            failed = false;
        } catch (JedisException e) {
            message = e.getMessage();
            failed = true;
        }
        return this;
    }
}
