package com.gotocompany.depot.config;

import org.aeonbits.owner.Config;

/**
 * Owner configuration interface for Depot's StatsD-based metrics reporting.
 *
 * <p>Depot instruments its sinks and parsers and publishes the resulting measurements to a StatsD
 * agent. This interface exposes the connection and tagging settings for that agent. Each accessor is
 * bound to an environment-style property through the Aeon Owner {@link Config.Key} annotation, and a
 * missing property falls back to the declared {@link Config.DefaultValue}.
 */
public interface MetricsConfig extends Config {

    /**
     * Returns the hostname or IP address of the StatsD agent that receives Depot's metrics.
     *
     * <p>Bound to the {@code METRIC_STATSD_HOST} property; defaults to {@code localhost}.
     *
     * @return the StatsD agent host
     */
    @Config.Key("METRIC_STATSD_HOST")
    @DefaultValue("localhost")
    String getMetricStatsDHost();

    /**
     * Returns the UDP port on which the StatsD agent listens for metrics.
     *
     * <p>Bound to the {@code METRIC_STATSD_PORT} property; defaults to {@code 8125}.
     *
     * @return the StatsD agent port
     */
    @Config.Key("METRIC_STATSD_PORT")
    @DefaultValue("8125")
    Integer getMetricStatsDPort();

    /**
     * Returns the global tag specification appended to every metric Depot emits.
     *
     * <p>Bound to the {@code METRIC_STATSD_TAGS} property; defaults to an empty string, meaning no
     * additional tags. The value is returned verbatim as supplied in configuration.
     *
     * @return the StatsD global tag string, or an empty string when none are configured
     */
    @Config.Key("METRIC_STATSD_TAGS")
    @DefaultValue("")
    String getMetricStatsDTags();
}
