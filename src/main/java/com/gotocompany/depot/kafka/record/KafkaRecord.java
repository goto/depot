package com.gotocompany.depot.kafka.record;

import com.gotocompany.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Immutable record produced by the parser, holding either the serialized key and value or the parsing error.
 */
@AllArgsConstructor
@Getter
public class KafkaRecord {
    private final long index;
    private final byte[] key;
    private final byte[] value;
    private final ErrorInfo errorInfo;
    private final String metadata;
    private final boolean valid;

    /**
     * Creates a valid record holding the serialized key and value.
     *
     * @param index    the position of the originating message in the batch
     * @param key      the serialized record key, or {@code null} when no key proto is configured
     * @param value    the serialized record value
     * @param metadata the message metadata used for logging
     * @return the valid record
     */
    public static KafkaRecord validRecord(long index, byte[] key, byte[] value, String metadata) {
        return new KafkaRecord(index, key, value, null, metadata, true);
    }

    /**
     * Creates an invalid record holding the parsing error.
     *
     * @param index     the position of the originating message in the batch
     * @param errorInfo the error describing why the message could not be converted
     * @param metadata  the message metadata used for logging
     * @return the invalid record
     */
    public static KafkaRecord invalidRecord(long index, ErrorInfo errorInfo, String metadata) {
        return new KafkaRecord(index, null, null, errorInfo, metadata, false);
    }

    /**
     * Returns a log-friendly representation that excludes the key and value bytes.
     *
     * @return the string representation containing the index and metadata
     */
    @Override
    public String toString() {
        return String.format("KafkaRecord: Index: %d, Metadata: %s", index, metadata);
    }
}
