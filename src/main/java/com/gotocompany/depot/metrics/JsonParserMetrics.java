package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

/**
 * Metric-name definitions for Depot's JSON message parsing.
 *
 * <p>{@code JsonParserMetrics} extends {@link SinkMetrics} with the metric name used to measure how
 * long JSON parsing takes. It is consumed by the JSON message parser ({@code JsonMessageParser}).
 *
 * <p>Its metric name builds on the inherited application prefix and {@code sink_} segment, with an
 * additional {@code json_parse_} segment supplied by {@link #JSON_PARSE_PREFIX}.
 */
public class JsonParserMetrics extends SinkMetrics {
    /**
     * Creates a JSON-parser metrics helper bound to the supplied sink configuration.
     *
     * @param config the sink configuration supplying the application metric prefix
     */
    public JsonParserMetrics(SinkConfig config) {
        super(config);
    }

    /**
     * Metric-name segment ({@code "json_parse_"}) inserted after the {@code sink_} segment for all
     * JSON-parsing metric names.
     */
    public static final String JSON_PARSE_PREFIX = "json_parse_";

    /**
     * Returns the metric name recording the time taken to parse a JSON message, in milliseconds.
     *
     * <p>Composed as {@code applicationPrefix + "sink_" + "json_parse_" + "operation_milliseconds"}.
     *
     * @return the JSON-parse time-taken metric name
     */
    public String getJsonParseTimeTakenMetric() {
        return getApplicationPrefix() + SINK_PREFIX + JSON_PARSE_PREFIX + "operation_milliseconds";
    }
}
