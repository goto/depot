package com.gotocompany.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.gotocompany.depot.message.Message;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * High-level client for streaming records into BigQuery through the Storage Write API.
 *
 * <p>This is the entry point used by the BigQuery sink when the Storage Write API path is enabled. It
 * separates the work into two phases:</p>
 * <ol>
 *     <li>{@link #convert(List)} transforms incoming {@link Message} objects into a
 *     {@link BigQueryPayload}, capturing per-record validity and the mapping between payload row
 *     indexes and original input indexes.</li>
 *     <li>{@link #appendAndGet(BigQueryPayload)} streams the converted payload to BigQuery and
 *     returns the server response.</li>
 * </ol>
 *
 * <p>Instances are created by {@link BigQueryStorageClientFactory} based on the configured schema
 * data type. The interface extends {@link Closeable} so callers can release the underlying writer and
 * any background resources.</p>
 *
 * @see com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStorageClient
 */
public interface BigQueryStorageClient extends Closeable {

    /**
     * Converts a batch of input messages into a single {@link BigQueryPayload} ready to be streamed.
     *
     * <p>Each message is parsed and mapped to the destination table schema. Records that fail
     * conversion are recorded as invalid within the returned payload (with their associated error
     * information) rather than aborting the whole batch, while valid records are accumulated into the
     * serialized payload.</p>
     *
     * @param messages the ordered batch of messages to convert; must not be {@code null}
     * @return a payload containing the serialized valid rows along with per-record metadata and the
     *         valid-index to input-index mapping
     */
    BigQueryPayload convert(List<Message> messages);

    /**
     * Streams the converted payload to BigQuery and blocks until the response is available.
     *
     * @param payload the payload produced by {@link #convert(List)} to append
     * @return the {@code AppendRowsResponse} returned by the Storage Write API
     * @throws ExecutionException   if the asynchronous append operation completes exceptionally
     * @throws InterruptedException if the current thread is interrupted while waiting for the result
     */
    AppendRowsResponse appendAndGet(BigQueryPayload payload) throws ExecutionException, InterruptedException;
}
