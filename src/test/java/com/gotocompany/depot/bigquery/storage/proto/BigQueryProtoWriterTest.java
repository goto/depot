package com.gotocompany.depot.bigquery.storage.proto;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStream;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterFactory;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

/**
 * Unit tests for {@link BigQueryProtoWriter}, the BigQuery Storage Write API stream-writer wrapper.
 *
 * <p>The fixture builds the writer through {@link BigQueryWriterFactory} with a mocked
 * {@link BigQueryWriteClient}, {@link CredentialsProvider} and {@link StreamWriter}, stubbing the
 * write stream to return a fixed {@link TableSchema}. The tests assert that {@code init} derives the
 * protobuf descriptor from the table schema, that {@code appendAndGet} forwards rows to the stream
 * writer, that {@code checkAndRefreshConnection} recreates the writer when the schema changes or the
 * writer is closed, and that the appropriate operation, latency and payload-size metrics are
 * emitted.</p>
 */
public class BigQueryProtoWriterTest {
    /** Mocked Storage Write API stream writer wrapped by the writer under test. */
    private final StreamWriter writer = Mockito.mock(StreamWriter.class);
    /** Mocked instrumentation used to verify metrics and logging. */
    private final Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
    /** Mocked sink configuration supplying project, dataset and table identifiers. */
    private final BigQuerySinkConfig config = Mockito.mock(BigQuerySinkConfig.class);
    /** Mocked metrics provider supplying operation, latency and payload metric names. */
    private final BigQueryMetrics metrics = Mockito.mock(BigQueryMetrics.class);
    /** Writer under test, created via {@link BigQueryWriterFactory} and initialized in {@link #setup()}. */
    private BigQueryProtoWriter bigQueryWriter;

    /**
     * Builds and initializes the writer under test against a mocked stream and a two-field schema.
     *
     * <p>Stubs the configuration and metrics, wires a {@link BigQueryProtoStream} around the mocked
     * {@link StreamWriter}, returns a {@link TableSchema} with a nullable string and a repeated int64
     * field from the write stream, and calls {@code init} on the created writer.</p>
     */
    @Before
    public void setup() {
        Mockito.when(config.getSinkConnectorSchemaDataType()).thenReturn(SinkConnectorSchemaDataType.PROTOBUF);
        Mockito.when(config.getGCloudProjectID()).thenReturn("test-project");
        Mockito.when(config.getDatasetName()).thenReturn("dataset");
        Mockito.when(config.getTableName()).thenReturn("table");
        Mockito.when(metrics.getBigqueryOperationTotalMetric()).thenReturn("application_sink_bigquery_operation_total");
        Mockito.when(metrics.getBigqueryOperationLatencyMetric()).thenReturn("application_sink_bigquery_operation_latency_milliseconds");
        BigQueryWriteClient bqwc = Mockito.mock(BigQueryWriteClient.class);
        CredentialsProvider cp = Mockito.mock(CredentialsProvider.class);
        BigQueryStream bqs = new BigQueryProtoStream(writer);
        WriteStream ws = Mockito.mock(WriteStream.class);
        TableSchema schema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field1")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field2")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .build();
        Mockito.when(ws.getTableSchema()).thenReturn(schema);
        Mockito.when(bqwc.getWriteStream(Mockito.any(GetWriteStreamRequest.class))).thenReturn(ws);
        bigQueryWriter = (BigQueryProtoWriter) BigQueryWriterFactory.createBigQueryWriter(config, c -> bqwc, c -> cp, (c, cr, p) -> bqs, instrumentation, metrics);
        bigQueryWriter.init();
    }

    /**
     * Verifies that initialization derives the descriptor from the table schema.
     *
     * <p>After {@code init}, asserts that the stream writer is exposed and the descriptor's fields
     * match the schema: a non-repeated string {@code field1} and a repeated int64 {@code field2}.</p>
     */
    @Test
    public void shouldInitStreamWriter() {
        Descriptors.Descriptor descriptor = bigQueryWriter.getDescriptor();
        Assert.assertEquals(writer, bigQueryWriter.getStreamWriter());
        Assert.assertEquals("field1", descriptor.getFields().get(0).getName());
        Assert.assertEquals(Descriptors.FieldDescriptor.Type.STRING, descriptor.getFields().get(0).getType());
        Assert.assertFalse(descriptor.getFields().get(0).isRepeated());
        Assert.assertEquals("field2", descriptor.getFields().get(1).getName());
        Assert.assertEquals(Descriptors.FieldDescriptor.Type.INT64, descriptor.getFields().get(1).getType());
        Assert.assertTrue(descriptor.getFields().get(1).isRepeated());
    }

    /**
     * Verifies that appending a payload forwards the rows and returns the response.
     *
     * <p>Given a payload wrapping {@link ProtoRows} and a stream writer whose append future resolves to
     * a response, when {@code appendAndGet} runs, then the returned response is the one produced by the
     * stream writer.</p>
     *
     * @throws Exception if the append future cannot be resolved
     */
    @Test
    public void shouldAppendAndGet() throws Exception {
        ProtoRows rows = Mockito.mock(ProtoRows.class);
        com.gotocompany.depot.bigquery.storage.BigQueryPayload payload = new BigQueryPayload();
        payload.setPayload(rows);
        ApiFuture<AppendRowsResponse> future = Mockito.mock(ApiFuture.class);
        AppendRowsResponse apiResponse = Mockito.mock(AppendRowsResponse.class);
        Mockito.when(future.get()).thenReturn(apiResponse);
        Mockito.when(writer.append(rows)).thenReturn(future);
        AppendRowsResponse appendRowsResponse = bigQueryWriter.appendAndGet(payload);
        Assert.assertEquals(apiResponse, appendRowsResponse);
    }

    /**
     * Verifies that the stream writer is recreated when an updated schema is detected.
     *
     * <p>Given an updated three-field {@link TableSchema} reported by the writer, when
     * {@code checkAndRefreshConnection} then {@code appendAndGet} run, then the previous writer is
     * closed, the new descriptor exposes all three fields, the response is returned and the
     * schema-update log message is emitted.</p>
     *
     * @throws ExecutionException   if resolving the append future fails
     * @throws InterruptedException if waiting on the append future is interrupted
     */
    @Test
    public void shouldRecreateStreamWriter() throws ExecutionException, InterruptedException {
        //check previous schema
        Descriptors.Descriptor descriptor = bigQueryWriter.getDescriptor();
        Assert.assertEquals(2, descriptor.getFields().size());
        TableSchema newSchema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field1")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field2")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field3")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .build();

        Mockito.when(writer.getUpdatedSchema()).thenReturn(newSchema);
        com.gotocompany.depot.bigquery.storage.BigQueryPayload payload = new BigQueryPayload();
        ProtoRows rows = Mockito.mock(ProtoRows.class);
        payload.setPayload(rows);
        ApiFuture<AppendRowsResponse> future = Mockito.mock(ApiFuture.class);
        AppendRowsResponse apiResponse = Mockito.mock(AppendRowsResponse.class);
        Mockito.when(future.get()).thenReturn(apiResponse);
        Mockito.when(writer.append(rows)).thenReturn(future);
        bigQueryWriter.checkAndRefreshConnection();
        AppendRowsResponse appendRowsResponse = bigQueryWriter.appendAndGet(payload);
        Mockito.verify(writer, Mockito.times(1)).close();
        Assert.assertEquals(apiResponse, appendRowsResponse);
        descriptor = bigQueryWriter.getDescriptor();
        Assert.assertEquals(3, descriptor.getFields().size());
        Assert.assertEquals(writer, bigQueryWriter.getStreamWriter());
        Assert.assertEquals("field1", descriptor.getFields().get(0).getName());
        Assert.assertEquals(Descriptors.FieldDescriptor.Type.STRING, descriptor.getFields().get(0).getType());
        Assert.assertFalse(descriptor.getFields().get(0).isRepeated());
        Assert.assertEquals("field2", descriptor.getFields().get(1).getName());
        Assert.assertEquals(Descriptors.FieldDescriptor.Type.INT64, descriptor.getFields().get(1).getType());
        Assert.assertTrue(descriptor.getFields().get(1).isRepeated());
        Assert.assertEquals("field3", descriptor.getFields().get(2).getName());
        Assert.assertEquals(Descriptors.FieldDescriptor.Type.STRING, descriptor.getFields().get(2).getType());
        Assert.assertFalse(descriptor.getFields().get(2).isRepeated());
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Updated table schema detected, recreating stream writer");
    }

    /**
     * Verifies that append operations emit the stream-writer-append metrics.
     *
     * <p>Given a successful append, when {@code appendAndGet} runs, then the operation counter and the
     * latency timer are each recorded once with the table, dataset, project and
     * {@code STREAM_WRITER_APPEND} API tags.</p>
     *
     * @throws Exception if the append future cannot be resolved
     */
    @Test
    public void shouldCaptureMetricsForStreamWriterAppend() throws Exception {
        ProtoRows rows = Mockito.mock(ProtoRows.class);
        com.gotocompany.depot.bigquery.storage.BigQueryPayload payload = new BigQueryPayload();
        payload.setPayload(rows);
        ApiFuture<AppendRowsResponse> future = Mockito.mock(ApiFuture.class);
        AppendRowsResponse apiResponse = Mockito.mock(AppendRowsResponse.class);
        Mockito.when(future.get()).thenReturn(apiResponse);
        Mockito.when(writer.append(rows)).thenReturn(future);
        bigQueryWriter.appendAndGet(payload);

        String tableName = String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, config.getTableName());
        String datasetName = String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, config.getDatasetName());
        String projectId = String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, config.getGCloudProjectID());
        String apiTag = String.format(BigQueryMetrics.BIGQUERY_API_TAG, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_APPEND);

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(
                metrics.getBigqueryOperationTotalMetric(),
                tableName,
                datasetName,
                projectId,
                apiTag);

        Mockito.verify(instrumentation, Mockito.times(1)).captureDurationSince(
                Mockito.eq(metrics.getBigqueryOperationLatencyMetric()),
                Mockito.any(Instant.class),
                Mockito.eq(tableName),
                Mockito.eq(datasetName),
                Mockito.eq(projectId),
                Mockito.eq(apiTag));
    }

    /**
     * Verifies that the writer is created once when no updated schema is available.
     *
     * <p>Given no updated schema reported, when {@code appendAndGet} runs, then no schema-update log is
     * emitted and the {@code STREAM_WRITER_CREATED} operation counter and latency timer are each
     * recorded once (from {@code init}).</p>
     *
     * @throws Exception if the append future cannot be resolved
     */
    @Test
    public void shouldCaptureMetricsForStreamWriterCreatedOnceWhenUpdatedSchemaIsNotAvailable() throws Exception {
        ProtoRows rows = Mockito.mock(ProtoRows.class);
        com.gotocompany.depot.bigquery.storage.BigQueryPayload payload = new BigQueryPayload();
        payload.setPayload(rows);
        ApiFuture<AppendRowsResponse> future = Mockito.mock(ApiFuture.class);
        AppendRowsResponse apiResponse = Mockito.mock(AppendRowsResponse.class);
        Mockito.when(future.get()).thenReturn(apiResponse);
        Mockito.when(writer.append(rows)).thenReturn(future);
        bigQueryWriter.appendAndGet(payload);

        String tableName = String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, config.getTableName());
        String datasetName = String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, config.getDatasetName());
        String projectId = String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, config.getGCloudProjectID());
        String apiTag = String.format(BigQueryMetrics.BIGQUERY_API_TAG, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_CREATED);

        Mockito.verify(instrumentation, Mockito.times(0)).logInfo("Updated table schema detected, recreating stream writer");
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(
                metrics.getBigqueryOperationTotalMetric(),
                tableName,
                datasetName,
                projectId,
                apiTag);
        Mockito.verify(instrumentation, Mockito.times(1)).captureDurationSince(
                Mockito.eq(metrics.getBigqueryOperationLatencyMetric()),
                Mockito.any(),
                Mockito.eq(tableName),
                Mockito.eq(datasetName),
                Mockito.eq(projectId),
                Mockito.eq(apiTag));
    }

    /**
     * Verifies that the writer is created twice when an updated schema triggers recreation.
     *
     * <p>Given an updated schema reported, when {@code checkAndRefreshConnection} then
     * {@code appendAndGet} run, then the schema-update log is emitted once and the
     * {@code STREAM_WRITER_CREATED} operation counter and latency timer are each recorded twice (init
     * plus recreation).</p>
     *
     * @throws Exception if the append future cannot be resolved
     */
    @Test
    public void shouldCaptureMetricsForStreamWriterCreatedTwiceWhenUpdatedSchemaIsAvailable() throws Exception {
        TableSchema newSchema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field1")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field2")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field3")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .build();

        ProtoRows rows = Mockito.mock(ProtoRows.class);
        com.gotocompany.depot.bigquery.storage.BigQueryPayload payload = new BigQueryPayload();
        payload.setPayload(rows);
        ApiFuture<AppendRowsResponse> future = Mockito.mock(ApiFuture.class);
        AppendRowsResponse apiResponse = Mockito.mock(AppendRowsResponse.class);
        Mockito.when(future.get()).thenReturn(apiResponse);
        Mockito.when(writer.append(rows)).thenReturn(future);
        Mockito.when(writer.getUpdatedSchema()).thenReturn(newSchema);
        bigQueryWriter.checkAndRefreshConnection();
        bigQueryWriter.appendAndGet(payload);

        String tableName = String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, config.getTableName());
        String datasetName = String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, config.getDatasetName());
        String projectId = String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, config.getGCloudProjectID());
        String apiTag = String.format(BigQueryMetrics.BIGQUERY_API_TAG, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_CREATED);

        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Updated table schema detected, recreating stream writer");
        Mockito.verify(instrumentation, Mockito.times(2)).incrementCounter(
                metrics.getBigqueryOperationTotalMetric(),
                tableName,
                datasetName,
                projectId,
                apiTag);
        Mockito.verify(instrumentation, Mockito.times(2)).captureDurationSince(
                Mockito.eq(metrics.getBigqueryOperationLatencyMetric()),
                Mockito.any(),
                Mockito.eq(tableName),
                Mockito.eq(datasetName),
                Mockito.eq(projectId),
                Mockito.eq(apiTag));
    }

    /**
     * Verifies that closing the old writer during recreation emits the closed metric.
     *
     * <p>Given an updated schema reported, when {@code checkAndRefreshConnection} then
     * {@code appendAndGet} run, then the schema-update log is emitted once and the
     * {@code STREAM_WRITER_CLOSED} operation counter and latency timer are each recorded once.</p>
     *
     * @throws Exception if the append future cannot be resolved
     */
    @Test
    public void shouldCaptureMetricsForStreamWriterClosedWhenUpdatedSchemaIsAvailable() throws Exception {
        TableSchema newSchema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field1")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field2")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field3")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .build();

        ProtoRows rows = Mockito.mock(ProtoRows.class);
        com.gotocompany.depot.bigquery.storage.BigQueryPayload payload = new BigQueryPayload();
        payload.setPayload(rows);
        ApiFuture<AppendRowsResponse> future = Mockito.mock(ApiFuture.class);
        AppendRowsResponse apiResponse = Mockito.mock(AppendRowsResponse.class);
        Mockito.when(future.get()).thenReturn(apiResponse);
        Mockito.when(writer.append(rows)).thenReturn(future);
        Mockito.when(writer.getUpdatedSchema()).thenReturn(newSchema);
        bigQueryWriter.checkAndRefreshConnection();
        bigQueryWriter.appendAndGet(payload);

        String tableName = String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, config.getTableName());
        String datasetName = String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, config.getDatasetName());
        String projectId = String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, config.getGCloudProjectID());
        String apiTag = String.format(BigQueryMetrics.BIGQUERY_API_TAG, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_CLOSED);

        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Updated table schema detected, recreating stream writer");
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(
                metrics.getBigqueryOperationTotalMetric(),
                tableName,
                datasetName,
                projectId,
                apiTag);
        Mockito.verify(instrumentation, Mockito.times(1)).captureDurationSince(
                Mockito.eq(metrics.getBigqueryOperationLatencyMetric()),
                Mockito.any(),
                Mockito.eq(tableName),
                Mockito.eq(datasetName),
                Mockito.eq(projectId),
                Mockito.eq(apiTag));
    }

    /**
     * Verifies that appending records the BigQuery payload-size metric.
     *
     * <p>Given a successful append, when {@code appendAndGet} runs, then the payload-size count is
     * captured once with the table, dataset and project tags.</p>
     *
     * @throws Exception if the append future cannot be resolved
     */
    @Test
    public void shouldCaptureBigqueryPayloadSizeMetrics() throws Exception {
        ProtoRows rows = Mockito.mock(ProtoRows.class);
        com.gotocompany.depot.bigquery.storage.BigQueryPayload payload = new BigQueryPayload();
        payload.setPayload(rows);
        ApiFuture<AppendRowsResponse> future = Mockito.mock(ApiFuture.class);
        AppendRowsResponse apiResponse = Mockito.mock(AppendRowsResponse.class);
        Mockito.when(future.get()).thenReturn(apiResponse);
        Mockito.when(writer.append(rows)).thenReturn(future);
        bigQueryWriter.appendAndGet(payload);

        String tableName = String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, config.getTableName());
        String datasetName = String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, config.getDatasetName());
        String projectId = String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, config.getGCloudProjectID());

        Mockito.verify(instrumentation, Mockito.times(1)).captureCount(
                Mockito.eq(metrics.getBigqueryPayloadSizeMetrics()),
                Mockito.anyLong(),
                Mockito.eq(tableName),
                Mockito.eq(datasetName),
                Mockito.eq(projectId));
    }

    /**
     * Verifies that a closed stream writer is recreated on connection refresh.
     *
     * <p>Given the stream writer reporting itself as closed, when {@code checkAndRefreshConnection}
     * then {@code appendAndGet} run, then the {@code STREAM_WRITER_CREATED} operation counter and
     * latency timer are each recorded twice (once for {@code init} and once for the recreation).</p>
     *
     * @throws Exception if the append future cannot be resolved
     */
    @Test
    public void shouldRecreateUnRecoverableStreamWriter() throws Exception {
        Mockito.when(writer.isClosed()).thenReturn(true);
        ProtoRows rows = Mockito.mock(ProtoRows.class);
        com.gotocompany.depot.bigquery.storage.BigQueryPayload payload = new BigQueryPayload();
        payload.setPayload(rows);
        ApiFuture<AppendRowsResponse> future = Mockito.mock(ApiFuture.class);
        AppendRowsResponse apiResponse = Mockito.mock(AppendRowsResponse.class);
        Mockito.when(future.get()).thenReturn(apiResponse);
        Mockito.when(writer.append(rows)).thenReturn(future);
        bigQueryWriter.checkAndRefreshConnection();
        bigQueryWriter.appendAndGet(payload);

        String tableName = String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, config.getTableName());
        String datasetName = String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, config.getDatasetName());
        String projectId = String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, config.getGCloudProjectID());
        String apiTag = String.format(BigQueryMetrics.BIGQUERY_API_TAG, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_CREATED);
        // Created twice, one for init() and another for closed
        Mockito.verify(instrumentation, Mockito.times(2)).incrementCounter(
                metrics.getBigqueryOperationTotalMetric(),
                tableName,
                datasetName,
                projectId,
                apiTag);
        Mockito.verify(instrumentation, Mockito.times(2)).captureDurationSince(
                Mockito.eq(metrics.getBigqueryOperationLatencyMetric()),
                Mockito.any(),
                Mockito.eq(tableName),
                Mockito.eq(datasetName),
                Mockito.eq(projectId),
                Mockito.eq(apiTag));
    }
}
