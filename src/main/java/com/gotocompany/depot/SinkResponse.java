package com.gotocompany.depot;

import com.gotocompany.depot.error.ErrorInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregated result of pushing a batch of records through a {@link Sink}.
 *
 * <p>Every call to {@link Sink#pushToSink(java.util.List)} returns one {@code SinkResponse}. It acts
 * as a sparse, per-record error report: only the records that failed are recorded, each keyed by its
 * zero-based index within the submitted batch. Records absent from the map were delivered
 * successfully. This lets callers commit the successful records while routing only the failed ones
 * (for example to a dead-letter queue) for retry or inspection.</p>
 *
 * <p>Failures are stored as {@link ErrorInfo} values, which pair the originating exception with a
 * classified {@link com.gotocompany.depot.error.ErrorType}. The collection starts empty and is
 * populated by the sink as it discovers failures through {@link #addErrors(long, ErrorInfo)} or
 * {@link #addErrors(java.util.List, ErrorInfo)}.</p>
 *
 * @see Sink
 * @see ErrorInfo
 * @see com.gotocompany.depot.error.ErrorType
 */
public class SinkResponse {
    /**
     * Per-record failures keyed by the zero-based index of the message within the submitted batch.
     *
     * <p>Starts empty and holds an entry only for records that failed; a missing key denotes a record
     * that was delivered successfully.</p>
     */
    private final Map<Long, ErrorInfo> errors = new HashMap<>();

    /**
     * Returns error as a map whose keys are indexes of messages that failed to be pushed.
     * Each failed message index is associated with a {@link ErrorInfo}.
     *
     * <p>The keys are the zero-based indexes of the messages that failed within the batch passed to
     * {@link Sink#pushToSink(java.util.List)}, and each value is the {@link ErrorInfo} describing why
     * that message failed. Indexes absent from the map correspond to records delivered successfully.
     * The returned map is the live backing map, not a defensive copy.</p>
     *
     * @return the map from each failed message index to its {@link ErrorInfo}; empty when no record
     *     failed
     */
    public Map<Long, ErrorInfo> getErrors() {
        return errors;
    }

    /**
     * Returns error for the provided message index. If no error exists returns {@code null}.
     *
     * @param index the zero-based index of the message within the submitted batch
     * @return the {@link ErrorInfo} associated with {@code index}, or {@code null} if that message did
     *     not fail
     */
    public ErrorInfo getErrorsFor(long index) {
        return errors.get(index);
    }

    /**
     * Adds an error for the index.
     *
     * <p>Any failure previously stored under the same index is replaced.</p>
     *
     * @param index the zero-based index of the failed message within the submitted batch
     * @param errorInfo the failure detail to associate with {@code index}
     */
    public void addErrors(long index, ErrorInfo errorInfo) {
        errors.put(index, errorInfo);
    }

    /**
     * Adds uniform error for the indexes.
     *
     * <p>Convenience method that applies {@code errorInfo} to every index in {@code indexes} by
     * delegating to {@link #addErrors(long, ErrorInfo)} for each one. Useful when a single fault, such
     * as a rejected batch, invalidates a group of records uniformly.</p>
     *
     * @param indexes the zero-based indexes of the failed messages within the submitted batch
     * @param errorInfo the failure detail to associate with each index
     */
    public void addErrors(List<Long> indexes, ErrorInfo errorInfo) {
        indexes.forEach(index -> addErrors(index, errorInfo));
    }

    /**
     * Returns {@code true} if no row insertion failed, {@code false} otherwise. If {@code false}.
     * {@link #getErrors()} ()} returns an empty map.
     *
     * @return {@code true} if at least one failure has been recorded, {@code false} if every record
     *     was delivered successfully and {@link #getErrors()} is empty
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

}
