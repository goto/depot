package com.gotocompany.depot.bigquery.storage;

/**
 * Marker interface abstracting a BigQuery Storage Write API stream writer.
 *
 * <p>The BigQuery Storage Write API exposes different concrete writer types depending on the
 * serialization format used to stream rows. Depot wraps each of these underlying writers behind this
 * common type so that factories and writers can pass streams around without depending on a specific
 * implementation:</p>
 *
 * <ul>
 *     <li>{@link com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStream} wraps a
 *     {@code StreamWriter} for Protobuf based streaming.</li>
 *     <li>{@link com.gotocompany.depot.bigquery.storage.json.BigQueryJsonStream} wraps a
 *     {@code JsonStreamWriter} for JSON based streaming.</li>
 * </ul>
 *
 * <p>The interface deliberately declares no members; callers downcast to the concrete type to obtain
 * the wrapped writer. It mainly serves as the return type produced by the stream-creator function
 * supplied to {@link BigQueryWriterFactory} and {@link BigQueryWriterUtils}.</p>
 *
 * @see BigQueryWriterUtils#getStreamWriter(com.gotocompany.depot.config.BigQuerySinkConfig,
 *      com.google.api.gax.core.CredentialsProvider, com.google.cloud.bigquery.storage.v1.ProtoSchema)
 */
public interface BigQueryStream {
}
