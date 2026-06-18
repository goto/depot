package com.gotocompany.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;

import java.util.concurrent.ExecutionException;

/**
 * Abstraction over a BigQuery Storage Write API writer that streams already-converted rows to a
 * BigQuery table.
 *
 * <p>A writer owns the lifecycle of the underlying Storage Write API connection (for example the
 * {@code StreamWriter} used for Protobuf streaming). Implementations are expected to be created
 * through {@link BigQueryWriterFactory}, lazily connect during {@link #init()}, append batches via
 * {@link #appendAndGet(BigQueryPayload)} and release resources through {@link AutoCloseable#close()}.</p>
 *
 * <p>Known implementations:</p>
 * <ul>
 *     <li>{@link com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoWriter} for Protobuf
 *     payloads.</li>
 *     <li>{@link com.gotocompany.depot.bigquery.storage.json.BigQueryJsonWriter}, currently a
 *     no-op placeholder for JSON payloads.</li>
 * </ul>
 *
 * @see BigQueryStorageClient
 * @see BigQueryPayload
 */
public interface BigQueryWriter extends AutoCloseable {

    /**
     * Initialises the writer by establishing the Storage Write API connection and preparing the
     * stream schema.
     *
     * <p>Implementations typically fetch the destination table schema from BigQuery and build the
     * underlying stream writer. This method may be invoked again by implementations to recreate the
     * connection (for example after a schema change or an idle/closed stream).</p>
     */
    void init();

    /**
     * Appends the supplied payload to the BigQuery stream and blocks until the server responds.
     *
     * @param payload the converted rows and their index bookkeeping to append; must not be
     *                {@code null}
     * @return the {@code AppendRowsResponse} returned by the Storage Write API, which may carry a
     *         stream level error and/or per-row errors
     * @throws ExecutionException   if the asynchronous append operation completes exceptionally
     * @throws InterruptedException if the current thread is interrupted while waiting for the append
     *                              result
     */
    AppendRowsResponse appendAndGet(BigQueryPayload payload) throws ExecutionException, InterruptedException;
}
