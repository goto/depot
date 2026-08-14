package com.gotocompany.depot.metrics;

import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Thin wrapper around a {@link StatsDClient} that applies Depot's tagging conventions when emitting
 * metrics.
 *
 * <p>{@code StatsDReporter} is the lowest-level metric-publishing component in Depot. It is usually
 * created by {@link StatsDReporterBuilder} and consumed through {@link Instrumentation}. Every metric
 * name passed to a capture method is decorated with a set of global tags (supplied at construction
 * time) before being forwarded to the wrapped client, so all measurements share a common dimensional
 * context.
 *
 * <p>Tags follow a {@code key=value} convention. The global tag string is normalized at construction
 * by replacing any {@code :} separators with {@code =}, and per-call tags are appended verbatim. The
 * class implements {@link Closeable}; closing it stops the underlying StatsD client.
 */
public class StatsDReporter implements Closeable {

    /**
     * Underlying StatsD client to which every measurement is delegated.
     */
    private final StatsDClient client;
    /**
     * Comma-separated global tags appended to every metric, normalized to {@code key=value} form.
     */
    private final String globalTags;
    /**
     * Logger used to report lifecycle events such as closing of the StatsD connection.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(StatsDReporter.class);

    /**
     * Creates a reporter that decorates every emitted metric with the supplied global tags.
     *
     * <p>The provided tags are joined with commas and any {@code :} characters are replaced with
     * {@code =}, producing the normalized {@code key=value} tag string that is appended to all
     * metrics emitted through this reporter.
     *
     * @param client     the StatsD client used to publish metrics
     * @param globalTags the global tags applied to every metric; each entry may use either
     *                   {@code key:value} or {@code key=value} form
     */
    public StatsDReporter(StatsDClient client, String... globalTags) {
        this.client = client;
        this.globalTags = String.join(",", globalTags).replaceAll(":", "=");
    }

    /**
     * Returns the underlying StatsD client backing this reporter.
     *
     * @return the wrapped {@link StatsDClient}
     */
    public StatsDClient getClient() {
        return client;
    }

    /**
     * Adjusts the named counter metric by the supplied delta.
     *
     * @param metric the base metric name
     * @param delta  the value added to the counter for this measurement
     * @param tags   optional per-call tags, each in {@code key=value} form
     */
    public void captureCount(String metric, Long delta, String... tags) {
        client.count(withTags(metric, tags), delta);
    }

    /**
     * Records a timing (histogram) measurement for the supplied value.
     *
     * @param metric the base metric name
     * @param delta  the value recorded into the timing distribution
     * @param tags   optional per-call tags, each in {@code key=value} form
     */
    public void captureHistogram(String metric, long delta, String... tags) {
        client.time(withTags(metric, tags), delta);
    }

    /**
     * Records an execution-time measurement spanning from the supplied start time until now.
     *
     * <p>The elapsed duration is computed as the number of milliseconds between {@code startTime} and
     * the current instant.
     *
     * @param metric    the base metric name
     * @param startTime the instant from which elapsed time is measured
     * @param tags      optional per-call tags, each in {@code key=value} form
     */
    public void captureDurationSince(String metric, Instant startTime, String... tags) {
        client.recordExecutionTime(withTags(metric, tags), Duration.between(startTime, Instant.now()).toMillis());
    }

    /**
     * Records an execution-time measurement for an already-computed duration.
     *
     * @param metric   the base metric name
     * @param duration the elapsed time, in milliseconds, to record
     * @param tags     optional per-call tags, each in {@code key=value} form
     */
    public void captureDuration(String metric, long duration, String... tags) {
        client.recordExecutionTime(withTags(metric, tags), duration);
    }

    /**
     * Reports the current value of a gauge metric.
     *
     * @param metric the base metric name
     * @param value  the instantaneous gauge value to report
     * @param tags   optional per-call tags, each in {@code key=value} form
     */
    public void gauge(String metric, Integer value, String... tags) {
        client.gauge(withTags(metric, tags), value);
    }

    /**
     * Increments the named counter metric by one.
     *
     * <p>Equivalent to invoking {@link #captureCount(String, Long, String...)} with a delta of one.
     *
     * @param metric the base metric name
     * @param tags   optional per-call tags, each in {@code key=value} form
     */
    public void increment(String metric, String... tags) {
        captureCount(metric, 1L, tags);
    }

    /**
     * Records a set-value event against the named metric.
     *
     * <p>Used to emit discrete events (such as error occurrences) where the recorded sample is an
     * event name rather than a numeric value.
     *
     * @param metric    the base metric name
     * @param eventName the event name recorded as the set value
     * @param tags      optional per-call tags, each in {@code key=value} form
     */
    public void recordEvent(String metric, String eventName, String... tags) {
        client.recordSetValue(withTags(metric, tags), eventName);
    }

    /**
     * Appends the reporter's normalized global tags to the supplied metric name.
     *
     * @param metric the base metric name
     * @return the metric name followed by a comma and the global tag string
     */
    private String withGlobalTags(String metric) {
        return metric + "," + this.globalTags;
    }

    /**
     * Combines the metric name, the global tags, and any per-call tags into a single comma-separated
     * string suitable for the StatsD client.
     *
     * @param metric the base metric name
     * @param tags   the per-call tags appended after the global tags
     * @return the fully decorated metric string, with all components joined by commas
     */
    private String withTags(String metric, String... tags) {
        return Stream.concat(Stream.of(withGlobalTags(metric)), Stream.of(tags))
                .collect(Collectors.joining(","));
    }

    /**
     * Logs that the StatsD connection is closing and stops the underlying client.
     *
     * <p>{@inheritDoc}
     *
     * @throws IOException if closing the underlying client fails (declared by {@link Closeable})
     */
    @Override
    public void close() throws IOException {
        LOGGER.info("StatsD connection closed");
        client.stop();
    }

}
