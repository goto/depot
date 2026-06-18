package com.gotocompany.depot.bigquery;

import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.client.BigQueryRow;
import com.gotocompany.depot.bigquery.client.BigQueryRowWithInsertId;
import com.gotocompany.depot.bigquery.client.BigQueryRowWithoutInsertId;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.handler.ErrorHandler;
import com.gotocompany.depot.bigquery.handler.ErrorHandlerFactory;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClient;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClientFactory;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageResponseParser;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterFactory;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterUtils;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * Builds and wires the BigQuery {@link Sink} together with all of its collaborators.
 *
 * <p>This factory owns the construction of the BigQuery sink. {@link #init()} creates the
 * BigQuery client, metrics, error handler, schema update listener and message parser,
 * performs the initial schema synchronisation, and (when the Storage Write API is enabled)
 * sets up the storage client, writer and response parser. {@link #create()} then returns
 * either a {@link BigQueryStorageAPISink} or a {@link BigQuerySink} depending on the
 * configuration.</p>
 *
 * <p>Several convenience constructors are provided for callers that do not need to supply a
 * {@link StatsDReporter} or a custom row-id creator.</p>
 *
 * @see BigQuerySink
 * @see BigQueryStorageAPISink
 */
public class BigQuerySinkFactory {

    /** Reporter used to build instrumentation for the sink's collaborators. */
    private final StatsDReporter statsDReporter;
    /** Function that derives a row insert id from a record's column map; may be {@code null}. */
    private final Function<Map<String, Object>, String> rowIDCreator;
    /** Resolved BigQuery sink configuration. */
    private final BigQuerySinkConfig sinkConfig;
    /** BigQuery client created during {@link #init()}. */
    private BigQueryClient bigQueryClient;
    /** Strategy that turns a record into a BigQuery row, created during {@link #init()}. */
    private BigQueryRow rowCreator;
    /** Metrics for the BigQuery sink, created during {@link #init()}. */
    private BigQueryMetrics bigQueryMetrics;
    /** Error handler for legacy streaming inserts, created during {@link #init()}. */
    private ErrorHandler errorHandler;
    /** Cache holding the message-to-record converter, created during {@link #init()}. */
    private MessageRecordConverterCache converterCache;
    /** Storage Write API client, created during {@link #init()} when the Storage API is enabled. */
    private BigQueryStorageClient bigQueryStorageClient;
    /** Storage Write API response parser, created during {@link #init()} when the Storage API is enabled. */
    private BigQueryStorageResponseParser responseParser;

    /**
     * Creates the factory, building the sink configuration from an environment map.
     *
     * <p>Resolves a {@link BigQuerySinkConfig} from the supplied properties via
     * {@link ConfigFactory} and delegates to
     * {@link #BigQuerySinkFactory(BigQuerySinkConfig, StatsDReporter, Function)}.</p>
     *
     * @param env            the configuration key-value pairs used to build the sink config
     * @param statsDReporter the reporter used to build instrumentation
     * @param rowIDCreator   the function that derives a row insert id from a record's column
     *                       map, or {@code null} to disable insert ids
     */
    public BigQuerySinkFactory(Map<String, String> env, StatsDReporter statsDReporter, Function<Map<String, Object>, String> rowIDCreator) {
        this(ConfigFactory.create(BigQuerySinkConfig.class, env), statsDReporter, rowIDCreator);
    }

    /**
     * Creates the factory from a resolved configuration, reporter and row-id creator.
     *
     * @param sinkConfig     the resolved BigQuery sink configuration
     * @param statsDReporter the reporter used to build instrumentation
     * @param rowIDCreator   the function that derives a row insert id from a record's column
     *                       map, or {@code null} to disable insert ids
     */
    public BigQuerySinkFactory(BigQuerySinkConfig sinkConfig, StatsDReporter statsDReporter, Function<Map<String, Object>, String> rowIDCreator) {
        this.sinkConfig = sinkConfig;
        this.rowIDCreator = rowIDCreator;
        this.statsDReporter = statsDReporter;
    }

    /**
     * Creates the factory from a configuration only, with no-op metrics and no insert ids.
     *
     * <p>Uses a {@link StatsDReporter} backed by a {@link NoOpStatsDClient} and a
     * {@code null} row-id creator.</p>
     *
     * @param sinkConfig the resolved BigQuery sink configuration
     */
    public BigQuerySinkFactory(BigQuerySinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);
    }

    /**
     * Creates the factory from a configuration and reporter, with no insert ids.
     *
     * @param sinkConfig     the resolved BigQuery sink configuration
     * @param statsDReporter the reporter used to build instrumentation
     */
    public BigQuerySinkFactory(BigQuerySinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this(sinkConfig, statsDReporter, null);
    }


    /**
     * Creates the factory from a configuration and row-id creator, with no-op metrics.
     *
     * <p>Uses a {@link StatsDReporter} backed by a {@link NoOpStatsDClient}.</p>
     *
     * @param sinkConfig   the resolved BigQuery sink configuration
     * @param rowIDCreator the function that derives a row insert id from a record's column
     *                     map, or {@code null} to disable insert ids
     */
    public BigQuerySinkFactory(BigQuerySinkConfig sinkConfig, Function<Map<String, Object>, String> rowIDCreator) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), rowIDCreator);
    }


    /**
     * Initialises the BigQuery client and all collaborators needed to create the sink.
     *
     * <p>Builds the metrics, BigQuery client, converter cache, error handler, schema update
     * listener and message parser, then performs the initial schema synchronisation. The row
     * creator is chosen according to whether row insert ids are enabled. When the Storage
     * Write API is enabled, the BigQuery writer, storage client and storage response parser
     * are also created and initialised.</p>
     *
     * @throws IllegalArgumentException if initialisation fails because of an
     *                                  {@link IOException} while creating the sink
     */
    public void init() {
        try {
            this.bigQueryMetrics = new BigQueryMetrics(sinkConfig);
            this.bigQueryClient = new BigQueryClient(sinkConfig, bigQueryMetrics, new Instrumentation(statsDReporter, BigQueryClient.class));
            this.converterCache = new MessageRecordConverterCache();
            this.errorHandler = ErrorHandlerFactory.create(sinkConfig, bigQueryClient, statsDReporter);
            DepotStencilUpdateListener depotStencilUpdateListener = BigqueryStencilUpdateListenerFactory.create(sinkConfig, bigQueryClient, converterCache, statsDReporter);
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter, depotStencilUpdateListener);
            depotStencilUpdateListener.setMessageParser(messageParser);
            depotStencilUpdateListener.updateSchema();

            if (sinkConfig.isRowInsertIdEnabled()) {
                this.rowCreator = new BigQueryRowWithInsertId(rowIDCreator);
            } else {
                this.rowCreator = new BigQueryRowWithoutInsertId();
            }
            if (sinkConfig.getSinkBigqueryStorageAPIEnable()) {
                BigQueryWriter bigQueryWriter = BigQueryWriterFactory
                        .createBigQueryWriter(
                                sinkConfig,
                                BigQueryWriterUtils::getBigQueryWriterClient,
                                BigQueryWriterUtils::getCredentialsProvider,
                                BigQueryWriterUtils::getStreamWriter,
                                new Instrumentation(statsDReporter, BigQueryWriter.class),
                                bigQueryMetrics);
                bigQueryWriter.init();
                bigQueryStorageClient = BigQueryStorageClientFactory.createBigQueryStorageClient(sinkConfig, messageParser, bigQueryWriter);
                responseParser = new BigQueryStorageResponseParser(
                        sinkConfig,
                        new Instrumentation(statsDReporter, BigQueryStorageResponseParser.class),
                        bigQueryMetrics);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception occurred while creating sink", e);
        }
    }

    /**
     * Creates the configured BigQuery {@link Sink}.
     *
     * <p>Must be called after {@link #init()}. Returns a {@link BigQueryStorageAPISink} when
     * the Storage Write API is enabled, otherwise a {@link BigQuerySink} wired with the
     * client, converter cache, row creator, metrics, instrumentation and error handler.</p>
     *
     * @return the BigQuery sink implementation selected by the configuration
     */
    public Sink create() {
        if (sinkConfig.getSinkBigqueryStorageAPIEnable()) {
            return new BigQueryStorageAPISink(
                    bigQueryStorageClient,
                    responseParser);
        } else {
            return new BigQuerySink(
                    bigQueryClient,
                    converterCache,
                    rowCreator,
                    bigQueryMetrics,
                    new Instrumentation(statsDReporter, BigQuerySink.class),
                    errorHandler);
        }
    }
}
