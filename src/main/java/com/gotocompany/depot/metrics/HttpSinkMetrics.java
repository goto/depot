package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

/**
 * Metric-name and tag definitions for Depot's HTTP sink.
 *
 * <p>{@code HttpSinkMetrics} extends {@link SinkMetrics} with metric names specific to the HTTP sink,
 * such as the count of responses grouped by HTTP status code. It is consumed by HTTP sink components
 * such as {@code HttpSinkClient}.
 *
 * <p>Its metric names build on the inherited application prefix and {@code sink_} segment, with an
 * additional {@code http_} segment supplied by {@link #HTTP_SINK_PREFIX}.
 */
public class HttpSinkMetrics extends SinkMetrics {

    /**
     * Sink-specific metric-name segment ({@code "http_"}) inserted after the {@code sink_} segment for
     * all HTTP sink metric names.
     */
    public static final String HTTP_SINK_PREFIX = "http_";

    /**
     * Creates an HTTP-sink metrics helper bound to the supplied sink configuration.
     *
     * @param config the sink configuration supplying the application metric prefix
     */
    public HttpSinkMetrics(SinkConfig config) {
        super(config);
    }

    /**
     * Returns the metric name counting HTTP responses, typically tagged by response status code.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "http_" + "response_code_total"}.
     *
     * @return the HTTP response-code-total metric name
     */
    public String getHttpResponseCodeTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + HTTP_SINK_PREFIX + "response_code_total";
    }
}
