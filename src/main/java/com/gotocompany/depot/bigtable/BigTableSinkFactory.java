package com.gotocompany.depot.bigtable;

import com.gotocompany.depot.bigtable.client.BigTableClient;
import com.gotocompany.depot.bigtable.model.BigTableSchema;
import com.gotocompany.depot.bigtable.parser.BigTableRecordParser;
import com.gotocompany.depot.bigtable.parser.BigTableRowKeyParser;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.BigTableMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.utils.MessageConfigUtils;

import java.io.IOException;

/**
 * Factory that builds and wires a {@link BigTableSink}.
 *
 * <p>Construction follows Depot's two-phase pattern. The factory is created from a
 * {@link BigTableSinkConfig} and a {@link StatsDReporter}; {@link #init()} then establishes the
 * Bigtable connection and assembles the schema, client, parser and metrics, validating the target
 * table's schema in the process; finally {@link #create()} returns a ready {@link Sink}. Any failure
 * during {@link #init()} is surfaced as a {@link ConfigurationException}.</p>
 *
 * @see BigTableSink
 * @see BigTableClient
 */
public class BigTableSinkFactory {
    /** Bigtable sink configuration (project, instance, table, credentials and mappings). */
    private final BigTableSinkConfig sinkConfig;
    /** Reporter used to build instrumentation and the message parser. */
    private final StatsDReporter statsDReporter;
    /** Client created by {@link #init()} and shared with the created sink. */
    private BigTableClient bigTableClient;
    /** Record parser created by {@link #init()} and shared with the created sink. */
    private BigTableRecordParser bigTableRecordParser;
    /** Bigtable metric names and tags created by {@link #init()}. */
    private BigTableMetrics bigtableMetrics;

    /**
     * Creates a factory from Bigtable configuration and a metrics reporter.
     *
     * @param sinkConfig the Bigtable sink configuration
     * @param statsDReporter the reporter used for metrics and to build the message parser
     */
    public BigTableSinkFactory(BigTableSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    /**
     * Creates a factory that emits no metrics, using a no-op StatsD client.
     *
     * <p>Delegates to {@link #BigTableSinkFactory(BigTableSinkConfig, StatsDReporter)} with a
     * {@link StatsDReporter} backed by a {@link com.timgroup.statsd.NoOpStatsDClient}.</p>
     *
     * @param sinkConfig the Bigtable sink configuration
     */
    public BigTableSinkFactory(BigTableSinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()));
    }


    /**
     * Establishes the Bigtable connection and assembles the sink's collaborators.
     *
     * <p>Logs the effective Bigtable configuration, then builds the {@link BigTableSchema} from the
     * configured column-family mapping, the {@link BigTableMetrics}, and the {@link BigTableClient}.
     * The target table's schema is validated via {@link BigTableClient#validateBigTableSchema()} so a
     * missing table or missing column families fail fast. It then resolves the schema message mode and
     * the {@link MessageParser}, compiles the row-key {@link Template} into a
     * {@link BigTableRowKeyParser}, and constructs the {@link BigTableRecordParser}. Must be called once
     * before {@link #create()}.</p>
     *
     * @throws ConfigurationException if the Bigtable connection cannot be created or the row-key
     *     template is invalid, wrapping the underlying {@link java.io.IOException} or
     *     {@link InvalidTemplateException}
     */
    public void init() {
        try {
            Instrumentation instrumentation = new Instrumentation(statsDReporter, BigTableSinkFactory.class);
            String bigtableConfig = String.format("\n\tbigtable.gcloud.project = %s\n\tbigtable.instance = %s\n\tbigtable.table = %s"
                            + "\n\tbigtable.credential.path = %s\n\tbigtable.row.key.template = %s\n\tbigtable.column.family.mapping = %s\n\t",
                    sinkConfig.getGCloudProjectID(),
                    sinkConfig.getInstanceId(),
                    sinkConfig.getTableId(),
                    sinkConfig.getCredentialPath(),
                    sinkConfig.getRowKeyTemplate(),
                    sinkConfig.getColumnFamilyMapping());

            instrumentation.logInfo(bigtableConfig);
            BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
            bigtableMetrics = new BigTableMetrics(sinkConfig);
            bigTableClient = new BigTableClient(sinkConfig, bigtableSchema, bigtableMetrics, new Instrumentation(statsDReporter, BigTableClient.class));
            bigTableClient.validateBigTableSchema();

            Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter);

            Template keyTemplate = new Template(sinkConfig.getRowKeyTemplate());
            BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(keyTemplate);
            bigTableRecordParser = new BigTableRecordParser(
                    messageParser,
                    bigTableRowKeyParser,
                    modeAndSchema,
                    bigtableSchema);
            instrumentation.logInfo("Connection to bigtable established successfully");
        } catch (IOException | InvalidTemplateException e) {
            throw new ConfigurationException("Exception occurred while creating sink", e);
        }
    }

    /**
     * Builds a new {@link BigTableSink} from the collaborators assembled by {@link #init()}.
     *
     * <p>Each returned sink gets its own {@link Instrumentation} bound to {@link BigTableSink} and
     * shares the client, parser and metrics created during {@link #init()}, which must have been called
     * first.</p>
     *
     * @return a ready-to-use {@link Sink} that writes records to Bigtable
     */
    public Sink create() {
        return new BigTableSink(
                bigTableClient,
                bigTableRecordParser,
                bigtableMetrics,
                new Instrumentation(statsDReporter, BigTableSink.class));
    }
}
