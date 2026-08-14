package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

/**
 * Metric-name and tag definitions for Depot's MaxCompute sink.
 *
 * <p>{@code MaxComputeMetrics} extends {@link SinkMetrics} with the metric names and tag templates
 * specific to Alibaba Cloud MaxCompute (ODPS). It covers operation counts and latencies, flush
 * volume, payload conversion latency, unknown-field validation latency, streaming-insert session
 * lifecycle and missing-partition tracking, and it declares {@link MaxComputeAPIType} for tagging the
 * operation being measured. It is consumed by MaxCompute components such as {@code InsertManager}.
 *
 * <p>All metric names build on the inherited application prefix and {@code sink_} segment, with an
 * additional {@code maxcompute_} segment supplied by {@link #MAXCOMPUTE_SINK_PREFIX}.
 */
public class MaxComputeMetrics extends SinkMetrics {

    /**
     * Sink-specific metric-name segment ({@code "maxcompute_"}) inserted after the {@code sink_}
     * segment for all MaxCompute metric names.
     */
    public static final String MAXCOMPUTE_SINK_PREFIX = "maxcompute_";
    /**
     * Tag template ({@code "table=%s"}) whose placeholder is filled with the target MaxCompute table
     * name.
     */
    public static final String MAXCOMPUTE_TABLE_TAG = "table=%s";
    /**
     * Tag template ({@code "project=%s"}) whose placeholder is filled with the MaxCompute project
     * name.
     */
    public static final String MAXCOMPUTE_PROJECT_TAG = "project=%s";
    /**
     * Tag template ({@code "schema=%s"}) whose placeholder is filled with the MaxCompute schema name.
     */
    public static final String MAXCOMPUTE_SCHEMA_TAG = "schema=%s";
    /**
     * Tag template ({@code "api=%s"}) whose placeholder is filled with a {@link MaxComputeAPIType}
     * value identifying the operation.
     */
    public static final String MAXCOMPUTE_API_TAG = "api=%s";
    /**
     * Tag template ({@code "error=%s"}) whose placeholder is filled with a value classifying the
     * failure.
     */
    public static final String MAXCOMPUTE_ERROR_TAG = "error=%s";
    /**
     * Tag template ({@code "compression=%s-%s"}) whose two placeholders are filled with compression
     * details, such as the compression algorithm and its level or strategy.
     */
    public static final String MAXCOMPUTE_COMPRESSION_TAG = "compression=%s-%s";
    /**
     * Tag template ({@code "unknown_field_validation_type=%s"}) whose placeholder is filled with the
     * configured unknown-field validation type.
     */
    public static final String MAXCOMPUTE_UNKNOWN_FIELD_VALIDATION_TYPE_TAG = "unknown_field_validation_type=%s";

    /**
     * Creates a MaxCompute metrics helper bound to the supplied sink configuration.
     *
     * @param config the sink configuration supplying the application metric prefix
     */
    public MaxComputeMetrics(SinkConfig config) {
        super(config);
    }

    /**
     * Enumerates the MaxCompute operations that Depot instruments.
     *
     * <p>The selected constant is supplied as the {@code api} tag value (see
     * {@link #MAXCOMPUTE_API_TAG}) on the operation-total and operation-latency metrics.
     */
    public enum MaxComputeAPIType {
        /**
         * Update of an existing MaxCompute table's definition, such as a schema update.
         */
        TABLE_UPDATE,
        /**
         * Creation of a new MaxCompute table.
         */
        TABLE_CREATE,
        /**
         * Insertion of records into a MaxCompute table.
         */
        TABLE_INSERT,
    }

    /**
     * Returns the metric name counting MaxCompute operations performed.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "maxcompute_" + "operation_total"}.
     *
     * @return the MaxCompute operation-total metric name
     */
    public String getMaxComputeOperationTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + MAXCOMPUTE_SINK_PREFIX + "operation_total";
    }

    /**
     * Returns the metric name recording the latency of MaxCompute operations in milliseconds.
     *
     * <p>Composed as
     * {@code applicationPrefix + "sink_" + "maxcompute_" + "operation_latency_milliseconds"}.
     *
     * @return the MaxCompute operation-latency metric name
     */
    public String getMaxComputeOperationLatencyMetric() {
        return getApplicationPrefix() + SINK_PREFIX + MAXCOMPUTE_SINK_PREFIX + "operation_latency_milliseconds";
    }

    /**
     * Returns the metric name recording the size, in bytes, of each flush to MaxCompute.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "maxcompute_" + "flush_size_bytes"}.
     *
     * @return the MaxCompute flush-size metric name
     */
    public String getMaxComputeFlushSizeMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "flush_size_bytes");
    }

    /**
     * Returns the metric name recording the number of records in each flush to MaxCompute.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "maxcompute_" + "flush_record_count"}.
     *
     * @return the MaxCompute flush-record-count metric name
     */
    public String getMaxComputeFlushRecordMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "flush_record_count");
    }

    /**
     * Returns the metric name recording the latency of payload conversion in milliseconds.
     *
     * <p>Composed as
     * {@code applicationPrefix + "sink_" + "maxcompute_" + "payload_conversion_latency_milliseconds"}.
     *
     * @return the MaxCompute payload-conversion-latency metric name
     */
    public String getMaxComputeConversionLatencyMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "payload_conversion_latency_milliseconds");
    }

    /**
     * Returns the metric name recording the latency of unknown-field validation in milliseconds.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "maxcompute_"
     * + "unknown_field_validation_latency_milliseconds"}.
     *
     * @return the MaxCompute unknown-field-validation-latency metric name
     */
    public String getMaxComputeUnknownFieldValidationLatencyMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "unknown_field_validation_latency_milliseconds");
    }

    /**
     * Returns the metric name counting created streaming-insert sessions.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "maxcompute_"
     * + "streaming_insert_session_created_count"}.
     *
     * @return the MaxCompute streaming-insert-session-created-count metric name
     */
    public String getMaxComputeStreamingInsertSessionCreatedCount() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "streaming_insert_session_created_count");
    }

    /**
     * Returns the metric name recording the initialization latency of streaming-insert sessions in
     * milliseconds.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "maxcompute_"
     * + "streaming_insert_session_initialization_latency_milliseconds"}.
     *
     * @return the MaxCompute streaming-insert-session-initialization-latency metric name
     */
    public String getMaxComputeStreamingInsertSessionInitializationLatency() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "streaming_insert_session_initialization_latency_milliseconds");
    }

    /**
     * Returns the metric name counting records that could not be matched to a partition.
     *
     * <p>Composed as
     * {@code applicationPrefix + "sink_" + "maxcompute_" + "missing_partition_records_total"}.
     *
     * @return the MaxCompute missing-partition-records metric name
     */
    public String getMaxComputeMissingPartitionRecrodsMetric() {
        return String.format("%s%s%s%s", getApplicationPrefix(), SINK_PREFIX, MAXCOMPUTE_SINK_PREFIX, "missing_partition_records_total");
    }
}
