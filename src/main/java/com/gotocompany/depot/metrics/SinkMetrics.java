package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;
import lombok.Getter;

/**
 * Base class for the per-sink metric-name builders used throughout Depot.
 *
 * <p>{@code SinkMetrics} centralizes the naming conventions shared by every sink: an
 * application-specific prefix (configured via {@code SINK_METRICS_APPLICATION_PREFIX}), a common
 * {@code sink_} segment, and the error-reporting tags and event name used by {@link Instrumentation}
 * when capturing fatal and non-fatal errors. Concrete subclasses such as {@link BigQueryMetrics},
 * {@link BigTableMetrics}, {@link HttpSinkMetrics}, {@link MaxComputeMetrics},
 * {@link RedisSinkMetrics} and {@link JsonParserMetrics} add the sink-specific metric names on top of
 * this foundation.
 *
 * <p>Metric names are assembled by concatenating the application prefix with fixed prefix segments
 * and a metric suffix, while tag constants are {@link String#format(String, Object...)} templates
 * whose {@code %s} placeholders are filled in at emission time.
 */
public class SinkMetrics {
    /**
     * Common metric-name segment ({@code "sink_"}) shared by all sink metrics, inserted between the
     * application prefix and the sink-specific prefix.
     */
    public static final String SINK_PREFIX = "sink_";
    // ERROR TAGS
    /**
     * Tag template ({@code "error_type=%s"}) whose placeholder is filled with the error type when
     * categorizing failures.
     */
    public static final String ERROR_TYPE_TAG = "error_type=%s";
    /**
     * Metric-name segment ({@code "error_"}) used to compose error-related metric names such as the
     * error-event metric.
     */
    public static final String ERROR_PREFIX = "error_";
    /**
     * Tag key ({@code "class"}) under which the originating exception's class name is recorded on
     * error events.
     */
    public static final String ERROR_MESSAGE_CLASS_TAG = "class";
    /**
     * Error-severity marker ({@code "nonfatal"}) used as the event name and {@code type} tag value
     * for non-fatal errors.
     */
    public static final String NON_FATAL_ERROR = "nonfatal";
    /**
     * Error-severity marker ({@code "fatal"}) used as the event name and {@code type} tag value for
     * fatal errors.
     */
    public static final String FATAL_ERROR = "fatal";

    /**
     * Application-specific metric-name prefix resolved from configuration; prepended to every metric
     * name produced by this class and its subclasses.
     */
    @Getter
    private final String applicationPrefix;

    /**
     * Creates a metrics helper, capturing the application prefix from the supplied configuration.
     *
     * @param config the sink configuration whose configured metrics application prefix is used as the
     *               prefix for all metric names
     */
    public SinkMetrics(SinkConfig config) {
        this.applicationPrefix = config.getMetricsApplicationPrefix();
    }

    /**
     * Returns the metric name under which error events are recorded.
     *
     * <p>The name is composed as {@code applicationPrefix + "error_" + "event"}.
     *
     * @return the fully qualified error-event metric name
     */
    public String getErrorEventMetric() {
        return applicationPrefix + ERROR_PREFIX + "event";
    }
}
