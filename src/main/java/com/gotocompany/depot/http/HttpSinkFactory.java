package com.gotocompany.depot.http;

import com.gotocompany.depot.common.client.HttpClientUtils;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.client.HttpSinkClient;
import com.gotocompany.depot.http.request.Request;
import com.gotocompany.depot.http.request.RequestFactory;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.metrics.HttpSinkMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Factory that assembles a fully wired HTTP {@link Sink} from configuration.
 *
 * <p>The factory follows a two-phase lifecycle. Construction simply captures the
 * {@link HttpSinkConfig} and the {@link StatsDReporter} used for metrics. The heavyweight
 * collaborators are then built lazily by {@link #init()}, which creates the pooled HTTP client, the
 * message parser and the request-building strategy. Once {@link #init()} has run, each call to
 * {@link #create()} returns a new {@link HttpSink} sharing those collaborators.</p>
 *
 * <p>This separation lets callers construct the factory cheaply and defer the potentially failing
 * resource setup (HTTP client creation, template compilation) to an explicit initialization step.</p>
 *
 * @see HttpSink
 * @see HttpSinkClient
 * @see RequestFactory
 */
public class HttpSinkFactory {

    /**
     * Configuration describing the HTTP endpoint, request mode, body type and related settings.
     */
    private final HttpSinkConfig sinkConfig;
    /**
     * Reporter used to emit StatsD metrics for the constructed sink and its client.
     */
    private final StatsDReporter statsDReporter;

    /**
     * HTTP transport client built during {@link #init()} and shared by created sinks.
     */
    private HttpSinkClient httpSinkClient;
    /**
     * Request-building strategy built during {@link #init()} and shared by created sinks.
     */
    private Request request;

    /**
     * Creates a factory that reports metrics through the supplied reporter.
     *
     * @param sinkConfig the HTTP sink configuration
     * @param statsDReporter the reporter used to emit metrics for the sink and its client
     */
    public HttpSinkFactory(HttpSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    /**
     * Creates a factory that discards metrics by using a no-op StatsD client.
     *
     * <p>Useful for tests or embedded usage where metric reporting is not required.</p>
     *
     * @param sinkConfig the HTTP sink configuration
     */
    public HttpSinkFactory(HttpSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = new StatsDReporter(new NoOpStatsDClient());
    }

    /**
     * Builds the long-lived collaborators required to serve HTTP sinks.
     *
     * <p>Creates a pooled {@link org.apache.http.impl.client.CloseableHttpClient} from the
     * configuration, wraps it in an {@link HttpSinkClient} together with HTTP metrics, resolves the
     * appropriate {@link MessageParser} via {@link MessageParserFactory}, and constructs the request
     * strategy through {@link RequestFactory#create(HttpSinkConfig, MessageParser)}. Must be called
     * before {@link #create()}.</p>
     *
     * @throws IllegalArgumentException if the sink cannot be initialized because of a
     *     {@link ConfigurationException} or {@link InvalidTemplateException} raised while building the
     *     parser or request strategy
     */
    public void init() {
        try {
            CloseableHttpClient closeableHttpClient = HttpClientUtils.newHttpClient(sinkConfig, statsDReporter);
            HttpSinkMetrics httpSinkMetrics = new HttpSinkMetrics(sinkConfig);
            httpSinkClient = new HttpSinkClient(closeableHttpClient, httpSinkMetrics, new Instrumentation(statsDReporter, HttpSinkClient.class));
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter);
            request = RequestFactory.create(sinkConfig, messageParser);
        } catch (ConfigurationException | InvalidTemplateException e) {
            throw new IllegalArgumentException("Exception occurred while creating Http sink", e);
        }
    }

    /**
     * Creates a new {@link HttpSink} backed by the collaborators built during {@link #init()}.
     *
     * <p>The returned sink shares the initialized HTTP client and request strategy and is configured
     * with the retry and request-log status-code ranges drawn from the sink configuration, plus a
     * dedicated {@link Instrumentation} scoped to {@link HttpSink}.</p>
     *
     * @return a ready-to-use HTTP {@link Sink}
     */
    public Sink create() {
        return new HttpSink(
                httpSinkClient,
                request,
                sinkConfig.getSinkHttpRetryStatusCodeRanges(),
                sinkConfig.getSinkHttpRequestLogStatusCodeRanges(),
                new Instrumentation(statsDReporter, HttpSink.class)
        );
    }
}
