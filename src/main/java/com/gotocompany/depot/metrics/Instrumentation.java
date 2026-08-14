package com.gotocompany.depot.metrics;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;


/**
 * Instrumentation.
 * <p>
 * Handle logging and metric capturing.
 *
 * <p>{@code Instrumentation} wraps an SLF4J {@link Logger} together with a {@link StatsDReporter},
 * giving sink and parser code a single, convenient entry point for both application logging and
 * telemetry. The logging methods delegate to the underlying SLF4J logger, while the metric methods
 * ({@code captureCount}, {@code captureHistogram}, {@code incrementCounter}, {@code captureValue},
 * {@code captureDuration} and {@code captureDurationSince}) delegate to the wrapped
 * {@link StatsDReporter}.
 *
 * <p>The error-capture helpers additionally translate a {@link Throwable} into a StatsD event tagged
 * with the originating exception class and an error severity ({@link SinkMetrics#NON_FATAL_ERROR} or
 * {@link SinkMetrics#FATAL_ERROR}), so a failure is both logged and counted in a single call.
 *
 * <p>The class implements {@link Closeable}; closing it releases the underlying StatsD client held
 * by the wrapped reporter.
 */
public class Instrumentation implements Closeable {
    /**
     * Reporter to which every metric and error event emitted through this instance is forwarded.
     */
    @Getter
    private final StatsDReporter statsDReporter;
    /**
     * SLF4J logger used for all logging performed through this instance.
     */
    @Getter
    private final Logger logger;

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param logger         the logger
     */
    public Instrumentation(StatsDReporter statsDReporter, Logger logger) {
        this.statsDReporter = statsDReporter;
        this.logger = logger;
    }

    /**
     * Instantiates a new Instrumentation.
     *
     * <p>A logger named after {@code clazz} is obtained from
     * {@link LoggerFactory#getLogger(Class)}, which is the usual way callers attach instrumentation
     * to a specific component.
     *
     * @param statsDReporter the stats d reporter
     * @param clazz          the clazz
     */
    public Instrumentation(StatsDReporter statsDReporter, Class clazz) {
        this.statsDReporter = statsDReporter;
        this.logger = LoggerFactory.getLogger(clazz);
    }

    // =================== LOGGING ===================

    /**
     * Logs a message at {@code INFO} level using SLF4J parameterized formatting.
     *
     * @param template the SLF4J message template, where {@code {}} placeholders are substituted
     * @param t        the arguments substituted into the template, in declaration order
     */
    public void logInfo(String template, Object... t) {
        logger.info(template, t);
    }

    /**
     * Logs a message at {@code WARN} level using SLF4J parameterized formatting.
     *
     * @param template the SLF4J message template, where {@code {}} placeholders are substituted
     * @param t        the arguments substituted into the template, in declaration order
     */
    public void logWarn(String template, Object... t) {
        logger.warn(template, t);
    }

    /**
     * Logs a message at {@code DEBUG} level using SLF4J parameterized formatting.
     *
     * @param template the SLF4J message template, where {@code {}} placeholders are substituted
     * @param t        the arguments substituted into the template, in declaration order
     */
    public void logDebug(String template, Object... t) {
        logger.debug(template, t);
    }

    /**
     * Logs a message at {@code ERROR} level using SLF4J parameterized formatting.
     *
     * @param template the SLF4J message template, where {@code {}} placeholders are substituted
     * @param t        the arguments substituted into the template, in declaration order
     */
    public void logError(String template, Object... t) {
        logger.error(template, t);
    }

    /**
     * Indicates whether {@code DEBUG} level logging is currently enabled for the backing logger.
     *
     * <p>Callers can guard the construction of expensive debug messages with this check to avoid
     * unnecessary work when debug logging is disabled.
     *
     * @return {@code true} if the underlying logger has {@code DEBUG} level enabled, {@code false}
     *         otherwise
     */
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }


    // ===================== CountTelemetry =================

    /**
     * Records a count metric, adjusting the named counter by the supplied delta.
     *
     * @param metric the metric name to emit
     * @param count  the value added to the counter for this measurement
     * @param tags   optional StatsD tags, each in {@code key=value} form, attached to the metric
     */
    public void captureCount(String metric, Long count, String... tags) {
        statsDReporter.captureCount(metric, count, tags);
    }

    /**
     * Records a histogram (timing) metric for the supplied value.
     *
     * @param metric the metric name to emit
     * @param count  the value recorded into the histogram
     * @param tags   optional StatsD tags, each in {@code key=value} form, attached to the metric
     */
    public void captureHistogram(String metric, Long count, String... tags) {
        statsDReporter.captureHistogram(metric, count, tags);
    }

    /**
     * Increments the named counter metric by one.
     *
     * @param metric the metric name to emit
     * @param tags   optional StatsD tags, each in {@code key=value} form, attached to the metric
     */
    public void incrementCounter(String metric, String... tags) {
        statsDReporter.increment(metric, tags);
    }

    /**
     * Records a gauge metric set to the supplied instantaneous value.
     *
     * @param metric the metric name to emit
     * @param value  the current gauge value to report
     * @param tags   optional StatsD tags, each in {@code key=value} form, attached to the metric
     */
    public void captureValue(String metric, Integer value, String... tags) {
        statsDReporter.gauge(metric, value, tags);
    }

    /**
     * Records an execution-time metric measured from the supplied start instant until now.
     *
     * @param metric  the metric name to emit
     * @param instant the start time from which the elapsed duration is measured
     * @param tags    optional StatsD tags, each in {@code key=value} form, attached to the metric
     */
    public void captureDurationSince(String metric, Instant instant, String... tags) {
        statsDReporter.captureDurationSince(metric, instant, tags);
    }

    /**
     * Records an execution-time metric for an already-computed duration.
     *
     * @param metric   the metric name to emit
     * @param duration the elapsed time, in milliseconds, to record
     * @param tags     optional StatsD tags, each in {@code key=value} form, attached to the metric
     */
    public void captureDuration(String metric, long duration, String... tags) {
        statsDReporter.captureDuration(metric, duration, tags);
    }


    // =================== ERROR ===================

    /**
     * Logs a non-fatal error and emits a corresponding StatsD error event.
     *
     * <p>The supplied template and arguments are logged at {@code WARN} level, the throwable's
     * message and stack trace are also logged at {@code WARN} level, and an event is recorded against
     * {@code metric} carrying {@link SinkMetrics#NON_FATAL_ERROR} together with a tag identifying the
     * concrete exception class (see {@link #errorTag(Throwable, String)}).
     *
     * @param metric   the metric name under which the error event is recorded
     * @param e        the throwable describing the non-fatal failure
     * @param template the SLF4J message template logged at {@code WARN} level
     * @param t        the arguments substituted into the template, in declaration order
     */
    public void captureNonFatalError(String metric, Throwable e, String template, Object... t) {
        logger.warn(template, t);
        logger.warn(e.getMessage(), e);
        statsDReporter.recordEvent(metric, SinkMetrics.NON_FATAL_ERROR, errorTag(e, SinkMetrics.NON_FATAL_ERROR));
    }

    /**
     * Logs a fatal error and emits a corresponding StatsD error event.
     *
     * <p>The supplied template and arguments are logged at {@code ERROR} level, the throwable's
     * message and stack trace are also logged at {@code ERROR} level, and an event is recorded
     * against {@code metric} carrying {@link SinkMetrics#FATAL_ERROR} together with a tag identifying
     * the concrete exception class (see {@link #errorTag(Throwable, String)}).
     *
     * @param metric   the metric name under which the error event is recorded
     * @param e        the throwable describing the fatal failure
     * @param template the SLF4J message template logged at {@code ERROR} level
     * @param t        the arguments substituted into the template, in declaration order
     */
    public void captureFatalError(String metric, Throwable e, String template, Object... t) {
        logger.error(template, t);
        logger.error(e.getMessage(), e);
        statsDReporter.recordEvent(metric, SinkMetrics.FATAL_ERROR, errorTag(e, SinkMetrics.FATAL_ERROR));
    }

    /**
     * Builds the comma-separated StatsD tag string that describes an error event.
     *
     * <p>The result combines the {@link SinkMetrics#ERROR_MESSAGE_CLASS_TAG} key set to the
     * fully-qualified exception class name with a {@code type} key set to the supplied severity.
     *
     * @param e         the throwable whose concrete class name is embedded in the tag
     * @param errorType the error severity, such as {@link SinkMetrics#NON_FATAL_ERROR} or
     *                  {@link SinkMetrics#FATAL_ERROR}
     * @return the tag string describing the error, suitable for use as a StatsD tag
     */
    private String errorTag(Throwable e, String errorType) {
        return SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
    }
    // ===================== closing =================

    /**
     * Closes the underlying {@link StatsDReporter}, releasing its StatsD client resources.
     *
     * @throws IOException if closing the underlying reporter fails
     */
    public void close() throws IOException {
        statsDReporter.close();
    }
}
