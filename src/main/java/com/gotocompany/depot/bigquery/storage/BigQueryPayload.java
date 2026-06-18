package com.gotocompany.depot.bigquery.storage;

import com.gotocompany.depot.bigquery.storage.proto.BigQueryRecordMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Container holding the result of converting a batch of messages for the BigQuery Storage Write API.
 *
 * <p>A payload bundles three pieces of information produced while a
 * {@link BigQueryStorageClient#convert(java.util.List)} call processes a batch:</p>
 * <ul>
 *     <li>The serialized {@link #payload} of valid rows (typically a
 *     {@code com.google.cloud.bigquery.storage.v1.ProtoRows} instance) that is appended to the
 *     stream.</li>
 *     <li>Per-record {@link BigQueryRecordMeta} metadata for <em>every</em> input message (both valid
 *     and invalid), exposed through {@link Iterable} iteration so invalid records can be reported.</li>
 *     <li>A mapping from the position of a row inside the serialized payload (the "valid index") back
 *     to the index of the original message in the input batch (the "input index").</li>
 * </ul>
 *
 * <p>The valid-index to input-index mapping is essential because invalid records are skipped when
 * building the serialized payload, so the BigQuery row indexes no longer line up with the original
 * batch positions. The response parser uses {@link #getInputIndex(long)} to translate per-row errors
 * reported by BigQuery back to the offending input message.</p>
 *
 * <p>This class is not thread-safe; a payload is expected to be built and consumed within a single
 * batch-processing flow.</p>
 *
 * @see BigQueryRecordMeta
 * @see BigQueryStorageResponseParser
 */
public class BigQueryPayload implements Iterable<BigQueryRecordMeta> {
    /** Per-record metadata for every input message in the batch, in input order. */
    private final List<BigQueryRecordMeta> recordMetadata = new ArrayList<>();
    /** Mapping from a row's index in the serialized payload to the original input message index. */
    private final Map<Long, Long> payloadIndexToInputIndex = new HashMap<>();
    /** The serialized rows ready to be appended to BigQuery (for example {@code ProtoRows}). */
    private Object payload;

    /**
     * Records the conversion metadata for a single input message.
     *
     * <p>This is called once per input message, regardless of whether the message converted
     * successfully, so the metadata list mirrors the full input batch in order.</p>
     *
     * @param record the per-record metadata describing validity and any conversion error
     */
    public void addMetadataRecord(BigQueryRecordMeta record) {
        recordMetadata.add(record);
    }

    /**
     * Registers the mapping between a valid row's position in the serialized payload and its original
     * position in the input batch.
     *
     * @param validIndex the zero-based index of the row within the serialized payload
     * @param inputIndex the zero-based index of the corresponding message in the input batch
     */
    public void putValidIndexToInputIndex(long validIndex, long inputIndex) {
        payloadIndexToInputIndex.put(validIndex, inputIndex);
    }

    /**
     * Returns the original input-batch index for the given payload row index.
     *
     * @param payloadIndex the index of a row within the serialized payload
     * @return the index of the corresponding message in the original input batch
     * @throws NullPointerException if no mapping exists for the supplied payload index
     */
    public long getInputIndex(long payloadIndex) {
        return payloadIndexToInputIndex.get(payloadIndex);
    }

    /**
     * Returns the set of payload row indexes for which a valid row was appended.
     *
     * @return the keys of the valid-index to input-index mapping; the size equals the number of valid
     *         rows in the serialized payload
     */
    public Set<Long> getPayloadIndexes() {
        return payloadIndexToInputIndex.keySet();
    }

    /**
     * Returns an iterator over the per-record metadata for the whole batch.
     *
     * @return an iterator over the {@link BigQueryRecordMeta} entries in input order
     */
    public Iterator<BigQueryRecordMeta> iterator() {
        return recordMetadata.iterator();
    }

    /**
     * Returns the serialized rows to be appended to BigQuery.
     *
     * @return the serialized payload object (for example a {@code ProtoRows} instance), or
     *         {@code null} if it has not been set yet
     */
    public Object getPayload() {
        return payload;
    }

    /**
     * Sets the serialized rows to be appended to BigQuery.
     *
     * @param payload the serialized payload object (for example a {@code ProtoRows} instance)
     */
    public void setPayload(Object payload) {
        this.payload = payload;
    }

}
