package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

/**
 * Metric-name and tag definitions for Depot's BigQuery sink.
 *
 * <p>{@code BigQueryMetrics} extends {@link SinkMetrics} with the metric names and tag templates
 * specific to BigQuery, covering both the legacy insert-all API and the BigQuery Storage Write API.
 * It also declares enums that enumerate the BigQuery operations, Storage Write API lifecycle events,
 * Storage Write API errors and error classifications that are attached as tags when metrics are
 * emitted (for example by {@code BigQueryClient} and {@code BigQueryProtoWriter}).
 *
 * <p>All metric names build on the inherited application prefix and {@code sink_} segment, with an
 * additional {@code bigquery_} segment supplied by {@link #BIGQUERY_SINK_PREFIX}.
 */
public class BigQueryMetrics extends SinkMetrics {

    /**
     * Creates a BigQuery metrics helper bound to the supplied sink configuration.
     *
     * @param config the sink configuration supplying the application metric prefix
     */
    public BigQueryMetrics(SinkConfig config) {
        super(config);
    }

    /**
     * Enumerates the BigQuery control-plane and legacy data-plane operations that Depot instruments.
     *
     * <p>The selected constant is supplied as the {@code api} tag value (see
     * {@link #BIGQUERY_API_TAG}) on the operation-total and operation-latency metrics.
     */
    public enum BigQueryAPIType {
        /**
         * Update of an existing BigQuery table's definition, such as a schema update.
         */
        TABLE_UPDATE,
        /**
         * Creation of a new BigQuery table.
         */
        TABLE_CREATE,
        /**
         * Update of an existing BigQuery dataset.
         */
        DATASET_UPDATE,
        /**
         * Creation of a new BigQuery dataset.
         */
        DATASET_CREATE,
        /**
         * Insertion of rows through BigQuery's legacy {@code insertAll} streaming API.
         */
        TABLE_INSERT_ALL,
    }

    /**
     * Enumerates the lifecycle events of a BigQuery Storage Write API stream writer.
     *
     * <p>The selected constant is supplied as the {@code api} tag value on Storage Write API
     * operation metrics.
     */
    public enum BigQueryStorageAPIType {
        /**
         * A new Storage Write API stream writer was created.
         */
        STREAM_WRITER_CREATED,
        /**
         * An existing Storage Write API stream writer was closed.
         */
        STREAM_WRITER_CLOSED,
        /**
         * Rows were appended to a stream through the Storage Write API.
         */
        STREAM_WRITER_APPEND
    }

    /**
     * Enumerates error categories specific to the BigQuery Storage Write API.
     */
    public enum BigQueryStorageAPIError {
        /**
         * An error occurred while appending rows through the Storage Write API.
         */
        ROW_APPEND_ERROR
    }

    /**
     * Enumerates the high-level error classifications reported for BigQuery write failures.
     *
     * <p>These values categorize failures surfaced by Depot's BigQuery error parsing and are attached
     * as the {@code error} tag (see {@link #BIGQUERY_ERROR_TAG}) on the error-total metric.
     */
    public enum BigQueryErrorType {
        /**
         * A failure that could not be classified into a more specific category.
         */
        UNKNOWN_ERROR,
        /**
         * A failure caused by a row not matching the destination table schema, such as referencing a
         * field that does not exist.
         */
        INVALID_SCHEMA_ERROR,
        /**
         * An out-of-bounds partitioning failure, where the partition column value falls outside the
         * table's allowed partition window.
         */
        OOB_ERROR,
        /**
         * A "stopped" failure, where a row was not inserted because other rows in the same batch
         * failed; such rows can typically be retried.
         */
        STOPPED_ERROR,
    }

    /**
     * Sink-specific metric-name segment ({@code "bigquery_"}) inserted after the {@code sink_}
     * segment for all BigQuery metric names.
     */
    public static final String BIGQUERY_SINK_PREFIX = "bigquery_";
    /**
     * Tag template ({@code "table=%s"}) whose placeholder is filled with the target BigQuery table
     * name.
     */
    public static final String BIGQUERY_TABLE_TAG = "table=%s";
    /**
     * Tag template ({@code "dataset=%s"}) whose placeholder is filled with the target BigQuery dataset
     * name.
     */
    public static final String BIGQUERY_DATASET_TAG = "dataset=%s";
    /**
     * Tag template ({@code "project=%s"}) whose placeholder is filled with the GCP project identifier.
     */
    public static final String BIGQUERY_PROJECT_TAG = "project=%s";
    /**
     * Tag template ({@code "api=%s"}) whose placeholder is filled with a {@link BigQueryAPIType} or
     * {@link BigQueryStorageAPIType} value identifying the operation.
     */
    public static final String BIGQUERY_API_TAG = "api=%s";
    /**
     * Tag template ({@code "error=%s"}) whose placeholder is filled with a {@link BigQueryErrorType}
     * value classifying the failure.
     */
    public static final String BIGQUERY_ERROR_TAG = "error=%s";

    /**
     * Returns the metric name counting BigQuery operations performed.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "bigquery_" + "operation_total"}.
     *
     * @return the BigQuery operation-total metric name
     */
    public String getBigqueryOperationTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGQUERY_SINK_PREFIX + "operation_total";
    }

    /**
     * Returns the metric name recording the latency of BigQuery operations in milliseconds.
     *
     * <p>Composed as
     * {@code applicationPrefix + "sink_" + "bigquery_" + "operation_latency_milliseconds"}.
     *
     * @return the BigQuery operation-latency metric name
     */
    public String getBigqueryOperationLatencyMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGQUERY_SINK_PREFIX + "operation_latency_milliseconds";
    }

    /**
     * Returns the metric name counting total BigQuery errors.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "bigquery_" + "errors_total"}.
     *
     * @return the BigQuery total-errors metric name
     */
    public String getBigqueryTotalErrorsMetrics() {
        return getApplicationPrefix() + SINK_PREFIX + BIGQUERY_SINK_PREFIX + "errors_total";
    }

    /**
     * Returns the metric name recording the size, in bytes, of BigQuery payloads.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "bigquery_" + "payload_size_bytes"}.
     *
     * @return the BigQuery payload-size metric name
     */
    public String getBigqueryPayloadSizeMetrics() {
        return getApplicationPrefix() + SINK_PREFIX + BIGQUERY_SINK_PREFIX + "payload_size_bytes";
    }

}
