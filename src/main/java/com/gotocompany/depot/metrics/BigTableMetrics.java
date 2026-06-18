package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

/**
 * Metric-name and tag definitions for Depot's Bigtable sink.
 *
 * <p>{@code BigTableMetrics} extends {@link SinkMetrics} with the metric names and tag templates
 * specific to Google Cloud Bigtable. It also declares {@link BigTableErrorType}, which classifies the
 * failures attached as the {@code error} tag (see {@link #BIGTABLE_ERROR_TAG}) when error metrics are
 * emitted (for example by {@code BigTableResponseParser}).
 *
 * <p>All metric names build on the inherited application prefix and {@code sink_} segment, with an
 * additional {@code bigtable_} segment supplied by {@link #BIGTABLE_SINK_PREFIX}.
 */
public class BigTableMetrics extends SinkMetrics {

    /**
     * Sink-specific metric-name segment ({@code "bigtable_"}) inserted after the {@code sink_}
     * segment for all Bigtable metric names.
     */
    public static final String BIGTABLE_SINK_PREFIX = "bigtable_";
    /**
     * Tag template ({@code "instance=%s"}) whose placeholder is filled with the Bigtable instance
     * identifier.
     */
    public static final String BIGTABLE_INSTANCE_TAG = "instance=%s";
    /**
     * Tag template ({@code "table=%s"}) whose placeholder is filled with the target Bigtable table
     * name.
     */
    public static final String BIGTABLE_TABLE_TAG = "table=%s";
    /**
     * Tag template ({@code "error=%s"}) whose placeholder is filled with a {@link BigTableErrorType}
     * value classifying the failure.
     */
    public static final String BIGTABLE_ERROR_TAG = "error=%s";

    /**
     * Creates a Bigtable metrics helper bound to the supplied sink configuration.
     *
     * @param config the sink configuration supplying the application metric prefix
     */
    public BigTableMetrics(SinkConfig config) {
        super(config);
    }


    /**
     * Enumerates the failure categories reported for Bigtable mutations.
     *
     * <p>The selected constant is attached as the {@code error} tag (see {@link #BIGTABLE_ERROR_TAG})
     * on Bigtable error metrics.
     */
    public enum BigTableErrorType {
        /**
         * A quota check failed, for example because a Bigtable usage limit was exceeded.
         */
        QUOTA_FAILURE, // A quota check failed.
        /**
         * One or more preconditions required for the request were not satisfied.
         */
        PRECONDITION_FAILURE, // Some preconditions have failed.
        /**
         * The client request was invalid, for example because it contained malformed input.
         */
        BAD_REQUEST, // Violations in a client request
        /**
         * The underlying RPC to Bigtable failed.
         */
        RPC_FAILURE,
    }

    /**
     * Returns the metric name recording the latency of Bigtable operations in milliseconds.
     *
     * <p>Composed as
     * {@code applicationPrefix + "sink_" + "bigtable_" + "operation_latency_milliseconds"}.
     *
     * @return the Bigtable operation-latency metric name
     */
    public String getBigtableOperationLatencyMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGTABLE_SINK_PREFIX + "operation_latency_milliseconds";
    }

    /**
     * Returns the metric name counting Bigtable operations performed.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "bigtable_" + "operation_total"}.
     *
     * @return the Bigtable operation-total metric name
     */
    public String getBigtableOperationTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGTABLE_SINK_PREFIX + "operation_total";
    }

    /**
     * Returns the metric name counting total Bigtable errors.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "bigtable_" + "errors_total"}.
     *
     * @return the Bigtable total-errors metric name
     */
    public String getBigtableTotalErrorsMetrics() {
        return getApplicationPrefix() + SINK_PREFIX + BIGTABLE_SINK_PREFIX + "errors_total";
    }
}
