package com.gotocompany.depot.log;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory that assembles and configures a {@link LogSink}.
 *
 * <p>Depot sinks are built in two phases. First a factory is constructed from configuration and a
 * {@link StatsDReporter}; then {@link #init()} resolves the heavier collaborators (here, the
 * {@link MessageParser} selected by {@link MessageParserFactory}); finally {@link #create()} returns a
 * ready-to-use {@link Sink}. Separating these phases lets configuration be validated and the parser be
 * built once and reused across created sinks.</p>
 *
 * @see LogSink
 * @see MessageParserFactory
 */
public class LogSinkFactory {

    /** Reporter used to build the {@link MessageParser} and the sink's {@link Instrumentation}. */
    private final StatsDReporter statsDReporter;
    /** Parser resolved by {@link #init()} and shared with every {@link LogSink} this factory creates. */
    private MessageParser messageParser;
    /** Resolved sink configuration driving parser selection and sink behavior. */
    private final SinkConfig sinkConfig;

    /**
     * Creates a factory from a raw environment map, materializing the {@link SinkConfig} from it.
     *
     * <p>The supplied environment entries are bound to a {@link SinkConfig} via the Owner
     * {@link org.aeonbits.owner.ConfigFactory} before delegating to
     * {@link #LogSinkFactory(SinkConfig, StatsDReporter)}.</p>
     *
     * @param env the configuration key-value pairs (typically process environment variables) to bind
     *     into a {@link SinkConfig}
     * @param statsDReporter the reporter used for metrics and for building the message parser
     */
    public LogSinkFactory(Map<String, String> env, StatsDReporter statsDReporter) {
        this(ConfigFactory.create(SinkConfig.class, env), statsDReporter);
    }

    /**
     * Creates a factory from an already-resolved configuration and reporter.
     *
     * @param sinkConfig the resolved sink configuration
     * @param statsDReporter the reporter used for metrics and for building the message parser
     */
    public LogSinkFactory(SinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    /**
     * Creates a factory that emits no metrics, using a no-op StatsD client.
     *
     * <p>Delegates to {@link #LogSinkFactory(SinkConfig, StatsDReporter)} with a {@link StatsDReporter}
     * backed by a {@link com.timgroup.statsd.NoOpStatsDClient}. Useful for tests and contexts where
     * telemetry is not required.</p>
     *
     * @param sinkConfig the resolved sink configuration
     */
    public LogSinkFactory(SinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()));
    }

    /**
     * Resolves the {@link MessageParser} used by sinks this factory creates.
     *
     * <p>Selects the parser matching the configured schema data type via
     * {@link MessageParserFactory#getParser(SinkConfig, StatsDReporter)} and stores it for reuse. Must
     * be called once before {@link #create()}.</p>
     */
    public void init() {
        this.messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter);
    }

    /**
     * Builds a new {@link LogSink} from the factory's configuration and resolved parser.
     *
     * <p>Each call returns a fresh sink with its own {@link Instrumentation} bound to {@link LogSink},
     * sharing the parser resolved by {@link #init()}. Call {@link #init()} before the first invocation
     * so the parser is available.</p>
     *
     * @return a ready-to-use {@link Sink} that logs parsed records
     */
    public Sink create() {
        return new LogSink(sinkConfig, messageParser, new Instrumentation(statsDReporter, LogSink.class));
    }
}
