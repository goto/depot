package com.gotocompany.depot.error;

/**
 * Classification of why a single record failed while being processed by a Depot sink.
 *
 * <p>Every {@link ErrorInfo} pairs the originating exception with one of these categories, and the
 * category is what downstream consumers (for example a host application's error-handling and
 * dead-letter logic) use to decide how to react: whether to retry, to drop, or to route the record
 * elsewhere. The constants distinguish failures that arise while turning raw bytes into a record
 * (deserialization, invalid content, unknown fields) from failures reported by the destination while
 * writing (the various {@code SINK_*} categories).</p>
 *
 * @see ErrorInfo
 */
public enum ErrorType {
    /**
     * The raw payload could not be deserialized into its in-memory representation, for example
     * because the bytes do not conform to the active schema or a logical type cannot be converted.
     */
    DESERIALIZATION_ERROR,
    /**
     * The record was structurally decodable but invalid for processing, such as an empty payload that
     * carries nothing to write.
     */
    INVALID_MESSAGE_ERROR,
    /**
     * The payload contained one or more fields that are not present in the configured schema.
     */
    UNKNOWN_FIELDS_ERROR,
    /**
     * The destination rejected the record with an HTTP 4xx-class status, indicating a client-side
     * problem with the request that will not be resolved by retrying it unchanged.
     */
    SINK_4XX_ERROR,
    /**
     * The destination responded with an HTTP 5xx-class status, indicating a server-side failure at
     * the destination.
     */
    SINK_5XX_ERROR,
    /**
     * The destination reported a transient failure that is safe to retry.
     */
    SINK_RETRYABLE_ERROR,
    /**
     * The destination reported a failure that must not be retried because replaying the record would
     * fail identically.
     */
    SINK_NON_RETRYABLE_ERROR,
    /**
     * The destination failed for a reason that does not map to any of the other categories.
     */
    SINK_UNKNOWN_ERROR,
    /**
     * Generic fallback category retained only for backward compatibility; deprecated and superseded by
     * the more specific error types above.
     */
    DEFAULT_ERROR //Deprecated
}
