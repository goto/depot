package com.gotocompany.depot.metrics;

import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import com.gotocompany.depot.config.MetricsConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * StatsDReporterFactory
 * <p>
 * Create statsDReporter Instance.
 *
 * <p>The builder resolves the StatsD agent host and port from configuration and constructs a
 * non-blocking StatsD client. If client creation fails for any reason it falls back to a
 * {@link NoOpStatsDClient}, so the application continues to run without metrics rather than failing.
 * The reporter's global tags are derived from the configured StatsD tags combined with any extra
 * tags supplied through {@link #withExtraTags(String...)}.
 *
 * <p>Instances are obtained via the static {@link #builder()} factory and configured fluently, for
 * example {@code StatsDReporterBuilder.builder().withMetricConfig(config).build()}.
 */
@Slf4j
public class StatsDReporterBuilder {

    /**
     * Metrics configuration supplying the StatsD host, port, and global tag specification.
     */
    private MetricsConfig metricsConfig;
    /**
     * Additional tags appended to the configured StatsD tags when the reporter is built.
     */
    private String[] extraTags;

    /**
     * Returns a copy of the supplied array enlarged by one slot, with {@code lastElement} appended.
     *
     * @param <T>         the component type of the array
     * @param arr         the source array to copy; its contents are left unchanged
     * @param lastElement the element appended at the end of the returned array
     * @return a new array containing all elements of {@code arr} followed by {@code lastElement}
     */
    private static <T> T[] append(T[] arr, T lastElement) {
        final int length = arr.length;
        arr = java.util.Arrays.copyOf(arr, length + 1);
        arr[length] = lastElement;
        return arr;
    }

    /**
     * Creates a new, empty builder instance.
     *
     * @return a fresh {@code StatsDReporterBuilder} ready to be configured
     */
    public static StatsDReporterBuilder builder() {
        return new StatsDReporterBuilder();
    }

    /**
     * Sets the metrics configuration used to build the reporter.
     *
     * @param config the metrics configuration supplying StatsD connection and tag settings
     * @return this builder, to allow fluent chaining
     */
    public StatsDReporterBuilder withMetricConfig(MetricsConfig config) {
        this.metricsConfig = config;
        return this;
    }

    /**
     * Sets additional tags to append to the reporter's configured global tags.
     *
     * @param tags the extra tags, each in {@code key=value} form, added to every emitted metric
     * @return this builder, to allow fluent chaining
     */
    public StatsDReporterBuilder withExtraTags(String... tags) {
        this.extraTags = tags;
        return this;
    }

    /**
     * Returns a copy of {@code arr} enlarged to hold all elements of {@code second}, appended in
     * order.
     *
     * @param <T>    the component type of the arrays
     * @param arr    the source array to copy; its contents are left unchanged
     * @param second the array whose elements are appended after those of {@code arr}
     * @return a new array containing the elements of {@code arr} followed by those of {@code second}
     */
    private static <T> T[] append(T[] arr, T[] second) {
        final int length = arr.length;
        arr = java.util.Arrays.copyOf(arr, length + second.length);
        System.arraycopy(second, 0, arr, length, second.length);
        return arr;
    }

    /**
     * Builds a {@link StatsDReporter} using the configured settings.
     *
     * <p>A StatsD client is created via {@link #buildStatsDClient()} and the reporter's global tags
     * are formed by splitting the configured StatsD tags value on commas and appending the extra tags
     * supplied through {@link #withExtraTags(String...)}.
     *
     * @return a new reporter wrapping the constructed StatsD client and resolved global tags
     */
    public StatsDReporter build() {
        StatsDClient statsDClient = buildStatsDClient();
        return new StatsDReporter(statsDClient, append(metricsConfig.getMetricStatsDTags().split(","), extraTags));
    }

    /**
     * Creates the underlying StatsD client from the configured host and port.
     *
     * <p>On success a non-blocking client built by {@link NonBlockingStatsDClientBuilder} is returned
     * and the connection is logged. If client creation throws for any reason, the failure is logged
     * and a {@link NoOpStatsDClient} is returned instead, allowing the application to continue running
     * without collecting metrics.
     *
     * @return a connected non-blocking StatsD client, or a {@link NoOpStatsDClient} if construction
     *         failed
     */
    private StatsDClient buildStatsDClient() {
        StatsDClient statsDClient;
        try {
            statsDClient = new NonBlockingStatsDClientBuilder()
                    .hostname(metricsConfig.getMetricStatsDHost())
                    .port(metricsConfig.getMetricStatsDPort())
                    .build();
            log.info("NonBlocking StatsD client connection established");
        } catch (Exception e) {
            log.warn("Exception on creating StatsD client, disabling StatsD and Audit client", e);
            log.warn("Application is running without collecting any metrics!!!!!!!!");
            statsDClient = new NoOpStatsDClient();
        }
        return statsDClient;
    }
}
